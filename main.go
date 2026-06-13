package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type BookData struct {
	Title       string
	Author      string
	Publisher   string
	Year        string
	Description string
	CoverURL    string
	GatewayURL  string
	DirectURL   string
}

// ---------------------------------------------------------
// ARCHIVE.ORG SESSION CACHE (For Interactive Menu)
// ---------------------------------------------------------
var (
	archiveSessions = make(map[string]*ArchiveSession)
	sessionMutex    sync.Mutex
)

type ArchiveSession struct {
	ItemID    string
	Title     string
	Author    string
	Publisher string
	CoverURL  string
	Files     []ArchiveFileMeta
	CreatedAt time.Time
}

type ArchiveFileMeta struct {
	Name   string `json:"name"`
	Format string `json:"format"`
	Size   string `json:"size"`
}

// ---------------------------------------------------------
// TEXT HELPERS
// ---------------------------------------------------------

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	clean := re.ReplaceAllString(name, "")

	// FIX: Strip existing extensions so we don't get filename.pdf.pdf or filename.epub.epub
	ext := filepath.Ext(clean)
	if ext != "" {
		clean = strings.TrimSuffix(clean, ext)
	}

	if len(clean) > 80 {
		clean = clean[:80]
	}
	return strings.TrimSpace(clean)
}

func formatArchiveSize(sizeStr string) float64 {
	bytes, _ := strconv.ParseFloat(sizeStr, 64)
	return bytes / (1024 * 1024)
}

// ---------------------------------------------------------
// GLOBAL TASK MANAGER (For Cancellation)
// ---------------------------------------------------------
var (
	activeTasks = make(map[string]context.CancelFunc)
	taskMutex   sync.Mutex
)

func cancelKeyboard(taskID string) *tgbotapi.InlineKeyboardMarkup {
	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", "cancel_"+taskID),
		),
	)
	return &kb
}

// ---------------------------------------------------------
// CANCELLABLE PROGRESS TRACKERS
// ---------------------------------------------------------
type DownloadProgress struct {
	Ctx        context.Context
	TaskID     string
	TotalSize  int64
	Downloaded int64
	Bot        *tgbotapi.BotAPI
	ChatID     int64
	MsgID      int
	LastUpdate time.Time
}

func (dp *DownloadProgress) Write(p []byte) (int, error) {
	if err := dp.Ctx.Err(); err != nil {
		return 0, err
	}

	n := len(p)
	dp.Downloaded += int64(n)

	now := time.Now()
	if now.Sub(dp.LastUpdate) >= 4*time.Second {
		dp.LastUpdate = now
		downloadedMB := float64(dp.Downloaded) / (1024 * 1024)
		var text string

		if dp.TotalSize > 0 {
			totalMB := float64(dp.TotalSize) / (1024 * 1024)
			percent := float64(dp.Downloaded) / float64(dp.TotalSize) * 100
			if percent > 100 {
				percent = 100
			}

			filled := int(percent / 10)
			if filled < 0 {
				filled = 0
			}
			if filled > 10 {
				filled = 10
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", 10-filled)

			text = fmt.Sprintf("📥 *Downloading File...*\n\n`%s` %.1f%%\n%.2f MB / %.2f MB", bar, percent, downloadedMB, totalMB)
		} else {
			text = fmt.Sprintf("📥 *Downloading File...*\n\n%.2f MB downloaded so far...", downloadedMB)
		}

		edit := tgbotapi.NewEditMessageText(dp.ChatID, dp.MsgID, text)
		edit.ParseMode = "Markdown"
		edit.ReplyMarkup = cancelKeyboard(dp.TaskID)
		_, _ = dp.Bot.Send(edit)
	}
	return n, nil
}

type UploadProgress struct {
	Ctx        context.Context
	TaskID     string
	Reader     io.Reader
	TotalSize  int64
	Uploaded   int64
	Bot        *tgbotapi.BotAPI
	ChatID     int64
	MsgID      int
	LastUpdate time.Time
}

func (up *UploadProgress) Read(p []byte) (int, error) {
	if err := up.Ctx.Err(); err != nil {
		return 0, err
	}

	n, err := up.Reader.Read(p)
	if n > 0 {
		up.Uploaded += int64(n)

		now := time.Now()
		if now.Sub(up.LastUpdate) >= 4*time.Second {
			up.LastUpdate = now
			percent := float64(up.Uploaded) / float64(up.TotalSize) * 100
			if percent > 100 {
				percent = 100
			}

			filled := int(percent / 10)
			if filled < 0 {
				filled = 0
			}
			if filled > 10 {
				filled = 10
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", 10-filled)

			uploadedMB := float64(up.Uploaded) / (1024 * 1024)
			totalMB := float64(up.TotalSize) / (1024 * 1024)

			text := fmt.Sprintf("📤 *Uploading to Telegram...*\n\n`%s` %.1f%%\n%.2f MB / %.2f MB", bar, percent, uploadedMB, totalMB)

			edit := tgbotapi.NewEditMessageText(up.ChatID, up.MsgID, text)
			edit.ParseMode = "Markdown"
			edit.ReplyMarkup = cancelKeyboard(up.TaskID)
			_, _ = up.Bot.Send(edit)
		}
	}
	return n, err
}

// ---------------------------------------------------------
// ENTRYPOINT
// ---------------------------------------------------------

func main() {
	go startSessionCleaner()
	botToken := os.Getenv("BOT_TOKEN")
	if botToken == "" {
		log.Panic("BOT_TOKEN environment variable is not set")
	}

	bot, err := tgbotapi.NewBotAPIWithAPIEndpoint(botToken, "http://telegram-bot-api.railway.internal:8081/bot%s/%s")
	if err != nil {
		log.Panicf("Failed to connect to Telegram: %v", err)
	}

	bot.Debug = true
	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.CallbackQuery != nil {
			go handleCallback(bot, update.CallbackQuery)
			continue
		}

		if update.Message == nil {
			continue
		}
		go handleUserMessage(bot, update.Message)
	}
}

func handleCallback(bot *tgbotapi.BotAPI, query *tgbotapi.CallbackQuery) {
	bot.Request(tgbotapi.NewCallback(query.ID, "Processing..."))

	// Handle Cancellations
	if strings.HasPrefix(query.Data, "cancel_") {
		taskID := strings.TrimPrefix(query.Data, "cancel_")
		taskMutex.Lock()
		if cancelFunc, exists := activeTasks[taskID]; exists {
			cancelFunc()
			delete(activeTasks, taskID)
		}
		taskMutex.Unlock()
		return
	}

	// Handle Archive.org Menu Buttons
	if strings.HasPrefix(query.Data, "ar|") {
		go processArchiveCallback(bot, query)
		return
	}
}

func processArchiveCallback(bot *tgbotapi.BotAPI, query *tgbotapi.CallbackQuery) {
	parts := strings.Split(query.Data, "|")
	if len(parts) < 3 {
		return
	}

	action := parts[1]
	sessionID := parts[2]

	sessionMutex.Lock()
	session, exists := archiveSessions[sessionID]
	sessionMutex.Unlock()

	if !exists {
		bot.Send(tgbotapi.NewMessage(query.Message.Chat.ID, "❌ Session expired. Please send the link again."))
		return
	}

	chatID := query.Message.Chat.ID

	// --- NEW: Remove the button they just clicked ---
	go removeClickedButton(bot, query)

	// Send the Cover and short metadata
	if action == "meta" {
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FileURL(session.CoverURL))

		caption := fmt.Sprintf("📖 Title: %s\n👤 Author: %s\n🏢 Publisher: %s",
			session.Title, session.Author, session.Publisher)

		photo.Caption = caption
		bot.Send(photo)
		return
	}

	if (action == "file" || action == "comp") && len(parts) == 4 {
		fileIdx, _ := strconv.Atoi(parts[3])
		if fileIdx < 0 || fileIdx >= len(session.Files) {
			return
		}

		f := session.Files[fileIdx]

		msgText := fmt.Sprintf("📥 Preparing to download %s...", filepath.Ext(f.Name))
		if action == "comp" {
			msgText = "📥 Preparing to download & compress file..."
		}
		statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, msgText))

		pathParts := strings.Split(f.Name, "/")
		for i, p := range pathParts {
			pathParts[i] = url.PathEscape(p)
		}

		book := &BookData{
			Title:     session.Title,
			DirectURL: "https://archive.org/download/" + session.ItemID + "/" + strings.Join(pathParts, "/"),
		}

		// Pass true if the user clicked "comp", pass false if they clicked "file"
		shouldCompress := action == "comp"
		go runDownloadPipeline(bot, chatID, statusMsg.MessageID, book, shouldCompress)
	}
}

func handleUserMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userText := strings.TrimSpace(msg.Text)

	if msg.IsCommand() && msg.Command() == "start" {
		bot.Send(tgbotapi.NewMessage(chatID, "👋 Send me any links (Libgen, Archive.org, or Direct file links) and I will download them!"))
		return
	}

	urlRegex := regexp.MustCompile(`https?://[^\s]+`)
	urls := urlRegex.FindAllString(userText, -1)

	if len(urls) == 0 {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ No valid HTTP/HTTPS links found."))
		return
	}

	for i, u := range urls {
		go processSingleLink(bot, chatID, u, i+1)
	}
}

func processSingleLink(bot *tgbotapi.BotAPI, chatID int64, linkURL string, itemNum int) {
	// 1. ROUTE ARCHIVE LINKS TO THE NEW INTERACTIVE MENU
	if strings.Contains(linkURL, "archive.org/details/") {
		handleArchiveMenu(bot, chatID, linkURL)
		return
	}

	// 2. ROUTE LIBGEN/DIRECT LINKS NORMALLY
	statusMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔍 Analyzing Link #%d...", itemNum))
	sentStatus, err := bot.Send(statusMsg)
	if err != nil {
		return
	}

	var book *BookData
	if strings.Contains(linkURL, "libgen.li") && !strings.HasSuffix(linkURL, ".pdf") && !strings.HasSuffix(linkURL, ".epub") {
		book, err = scrapeLibgen(linkURL)
	} else {
		parsedURL, _ := url.Parse(linkURL)
		filename := filepath.Base(parsedURL.Path)
		if unescaped, err := url.PathUnescape(filename); err == nil {
			filename = unescaped
		}
		book = &BookData{Title: sanitizeFilename(filename), DirectURL: linkURL}
	}

	if err != nil {
		editMessage(bot, chatID, sentStatus.MessageID, fmt.Sprintf("❌ Error processing Link #%d: %v", itemNum, err), "")
		return
	}

	runDownloadPipeline(bot, chatID, sentStatus.MessageID, book, false)
}

func handleArchiveMenu(bot *tgbotapi.BotAPI, chatID int64, pageURL string) {
	statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "🔍 Analyzing Archive.org item..."))

	parts := strings.Split(pageURL, "/details/")
	if len(parts) < 2 {
		editMessage(bot, chatID, statusMsg.MessageID, "❌ Malformed archive.org URL", "")
		return
	}
	id := strings.Split(strings.Split(parts[1], "/")[0], "?")[0]

	apiURL := "https://archive.org/metadata/" + id
	res, err := http.Get(apiURL)
	if err != nil || res.StatusCode != 200 {
		editMessage(bot, chatID, statusMsg.MessageID, "❌ Failed to fetch Archive data.", "")
		return
	}
	defer res.Body.Close()

	var archiveRes struct {
		Metadata map[string]interface{} `json:"metadata"`
		Files    []ArchiveFileMeta      `json:"files"`
	}
	json.NewDecoder(res.Body).Decode(&archiveRes)

	// Create session
	sessionID := fmt.Sprintf("%x", time.Now().UnixNano())[:8] // Short unique ID
	session := &ArchiveSession{
		ItemID:    id,
		Title:     cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "title")),
		Author:    cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "creator")),
		Publisher: cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "publisher")),
		CoverURL:  "https://archive.org/services/img/" + id,
		Files:     archiveRes.Files,
		CreatedAt: time.Now(),
	}
	if session.Title == "" {
		session.Title = "Archive_Item_" + id
	}

	sessionMutex.Lock()
	archiveSessions[sessionID] = session
	sessionMutex.Unlock()

	// Build Interactive Keyboard
	var keyboard [][]tgbotapi.InlineKeyboardButton

	// Row 1: The single Metadata button
	keyboard = append(keyboard, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("🖼 Cover + Metadata", "ar|meta|"+sessionID),
	))

	for i, f := range session.Files {
		lowerName := strings.ToLower(f.Name)
		if !strings.HasSuffix(lowerName, ".pdf") && !strings.HasSuffix(lowerName, ".epub") {
			continue
		}

		sizeMB := formatArchiveSize(f.Size)
		btnText := fmt.Sprintf("📄 PDF (%.1f MB)", sizeMB)
		if strings.HasSuffix(lowerName, ".epub") {
			btnText = fmt.Sprintf("📘 EPUB (%.1f MB)", sizeMB)
		}

		cbData := fmt.Sprintf("ar|file|%s|%d", sessionID, i)

		// Put the standard download button in the row
		row := []tgbotapi.InlineKeyboardButton{
			tgbotapi.NewInlineKeyboardButtonData(btnText, cbData),
		}

		// NEW: If it's a PDF and over 200MB, add a Compress button to the SAME row!
		if sizeMB > 200 && strings.HasSuffix(lowerName, ".pdf") {
			compCbData := fmt.Sprintf("ar|comp|%s|%d", sessionID, i)
			row = append(row, tgbotapi.NewInlineKeyboardButtonData("🗜 Compress", compCbData))
		}

		keyboard = append(keyboard, tgbotapi.NewInlineKeyboardRow(row...))
	}

	// Remove the "Analyzing" message and send the menu
	bot.Request(tgbotapi.NewDeleteMessage(chatID, statusMsg.MessageID))

	msg := tgbotapi.NewMessage(chatID, fmt.Sprintf("📖 *%s*\n\nChoose an option below:", session.Title))
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = tgbotapi.InlineKeyboardMarkup{InlineKeyboard: keyboard}
	bot.Send(msg)
}

func runDownloadPipeline(bot *tgbotapi.BotAPI, chatID int64, msgID int, book *BookData, shouldCompress bool) {
	ctx, cancel := context.WithCancel(context.Background())
	taskID := fmt.Sprintf("%d-%d", chatID, msgID)

	taskMutex.Lock()
	activeTasks[taskID] = cancel
	taskMutex.Unlock()

	defer func() {
		taskMutex.Lock()
		delete(activeTasks, taskID)
		taskMutex.Unlock()
		cancel()
	}()

	tmpDir, bookPath, _, err := downloadFiles(ctx, bot, chatID, msgID, taskID, book)
	if err != nil {
		if ctx.Err() != nil {
			editMessage(bot, chatID, msgID, "🚫 *Task Cancelled by User.*", "")
		} else {
			editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Download failed: %v", err), "")
		}
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		return
	}
	defer os.RemoveAll(tmpDir)
	// --- NEW: COMPRESSION LOGIC ---
	if shouldCompress && filepath.Ext(bookPath) == ".pdf" {
		editMessage(bot, chatID, msgID, "🗜 *Compressing PDF...*\nThis might take a minute depending on the file complexity.", taskID)

		compressedPath := filepath.Join(tmpDir, "compressed_"+filepath.Base(bookPath))
		err := compressPDF(bookPath, compressedPath)

		if err == nil {
			bookPath = compressedPath // Swap out the heavy file for the light one!
		} else {
			editMessage(bot, chatID, msgID, "⚠️ Compression failed! Uploading original size instead...", taskID)
			time.Sleep(2 * time.Second) // Let them read the warning
		}
	}

	editMessage(bot, chatID, msgID, "📤 Uploading file to Telegram...", taskID)

	file, err := os.Open(bookPath)
	if err != nil {
		editMessage(bot, chatID, msgID, "❌ Failed to read local file.", "")
		return
	}
	defer file.Close()

	stat, _ := file.Stat()
	uploadTracker := &UploadProgress{
		Ctx: ctx, TaskID: taskID, Reader: file, TotalSize: stat.Size(),
		Bot: bot, ChatID: chatID, MsgID: msgID, LastUpdate: time.Now(),
	}

	doc := tgbotapi.NewDocument(chatID, tgbotapi.FileReader{
		Name:   filepath.Base(bookPath),
		Reader: uploadTracker,
	})

	_, err = bot.Send(doc)
	if err != nil {
		if ctx.Err() != nil {
			editMessage(bot, chatID, msgID, "🚫 *Upload Cancelled by User.*", "")
		} else {
			editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Telegram upload failed: %v", err), "")
		}
		return
	}

	editMessage(bot, chatID, msgID, "✅ Upload Complete!", "")
}

// ---------------------------------------------------------
// ARCHIVE.ORG EXTRACTOR
// ---------------------------------------------------------

func getArchiveOrgData(pageURL string) (*BookData, error) {
	parts := strings.Split(pageURL, "/details/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("malformed archive.org URL")
	}
	id := strings.Split(strings.Split(parts[1], "/")[0], "?")[0]

	apiURL := "https://archive.org/metadata/" + id
	req, _ := http.NewRequest("GET", apiURL, nil)

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("network error: %v", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("API rejected request (Status %d)", res.StatusCode)
	}

	// FIX: Properly separated struct tags so the JSON decoder can read the filenames!
	var archiveRes struct {
		Metadata map[string]interface{} `json:"metadata"`
		Files    []struct {
			Name   string `json:"name"`
			Format string `json:"format"`
		} `json:"files"`
	}

	if err := json.NewDecoder(res.Body).Decode(&archiveRes); err != nil {
		return nil, fmt.Errorf("failed to parse JSON (Item may be restricted or blocked)")
	}

	book := &BookData{CoverURL: "https://archive.org/services/img/" + id}
	book.Title = cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "title"))
	if book.Title == "" {
		book.Title = "Archive_Item_" + id
	}
	book.Author = cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "creator"))
	book.Year = cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "date"))
	book.Publisher = cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "publisher"))
	book.Description = cleanText(stripHTML(extractDynamicInterfaceField(archiveRes.Metadata, "description")))

	var fallbackFile string
	for _, file := range archiveRes.Files {
		lowerName := strings.ToLower(file.Name)
		lowerFormat := strings.ToLower(file.Format)

		if strings.HasSuffix(lowerName, ".pdf") || strings.HasSuffix(lowerName, ".epub") ||
			strings.HasSuffix(lowerName, ".mobi") || strings.HasSuffix(lowerName, ".djvu") {

			pathParts := strings.Split(file.Name, "/")
			for i, p := range pathParts {
				pathParts[i] = url.PathEscape(p)
			}
			encodedPath := strings.Join(pathParts, "/")
			targetURL := "https://archive.org/download/" + id + "/" + encodedPath

			if strings.HasSuffix(lowerName, ".pdf") && (strings.Contains(lowerFormat, "text pdf") || strings.Contains(lowerName, "_text.pdf")) {
				fallbackFile = targetURL
			} else {
				book.DirectURL = targetURL
				break
			}
		}
	}

	if book.DirectURL == "" && fallbackFile != "" {
		book.DirectURL = fallbackFile
	}
	if book.DirectURL == "" {
		return nil, fmt.Errorf("no readable file (PDF/EPUB/MOBI/DJVU) found in this archive")
	}

	return book, nil
}

// ---------------------------------------------------------
// LIBGEN SCRAPER
// ---------------------------------------------------------

func scrapeLibgen(pageURL string) (*BookData, error) {
	req, _ := http.NewRequest("GET", pageURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("libgen returned error: %d", res.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
	}

	book := &BookData{}
	doc.Find("p").Each(func(i int, s *goquery.Selection) {
		text := cleanText(s.Text())
		if strings.HasPrefix(text, "Title:") {
			book.Title = strings.TrimSpace(strings.TrimPrefix(text, "Title:"))
		}
		if strings.HasPrefix(text, "Author(s):") {
			book.Author = strings.TrimSpace(strings.TrimPrefix(text, "Author(s):"))
		}
		if strings.HasPrefix(text, "Publisher:") {
			book.Publisher = strings.TrimSpace(strings.TrimPrefix(text, "Publisher:"))
		}
		if strings.HasPrefix(text, "Year:") {
			book.Year = strings.TrimSpace(strings.TrimPrefix(text, "Year:"))
		}
	})
	if book.Title == "" {
		book.Title = "Unknown_Libgen"
	}
	book.Description = cleanText(strings.ReplaceAll(doc.Find("div.col-12.order-5.float-left").Text(), "[...]", ""))

	doc.Find("div.col-xl-2 img").Each(func(i int, s *goquery.Selection) {
		if src, exists := s.Attr("src"); exists && strings.Contains(src, "/covers/") {
			book.CoverURL = "https://libgen.li" + src
		}
	})

	doc.Find("#tablelibgen td.valign-middle a").Each(func(i int, s *goquery.Selection) {
		if strings.ToLower(s.AttrOr("title", "")) == "libgen" {
			if href, exists := s.Attr("href"); exists {
				book.GatewayURL = href
				if !strings.HasPrefix(book.GatewayURL, "http") {
					if !strings.HasPrefix(book.GatewayURL, "/") {
						book.GatewayURL = "/" + book.GatewayURL
					}
					book.GatewayURL = "https://libgen.li" + book.GatewayURL
				}
			}
		}
	})
	if book.GatewayURL == "" {
		return nil, fmt.Errorf("no gateway link found")
	}
	return book, nil
}

// ---------------------------------------------------------
// UNIFIED DOWNLOAD PIPELINE
// ---------------------------------------------------------

func downloadFiles(ctx context.Context, bot *tgbotapi.BotAPI, chatID int64, msgID int, taskID string, book *BookData) (string, string, string, error) {
	tmpDir, err := os.MkdirTemp("", "library_*")
	if err != nil {
		return "", "", "", err
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{
		Timeout: 180 * time.Second,
		Jar:     jar,
	}

	var finalDownloadURL string

	if book.DirectURL != "" {
		finalDownloadURL = book.DirectURL
		editMessage(bot, chatID, msgID, "📥 *Connecting to File Server...*", taskID)
	} else {
		req, _ := http.NewRequestWithContext(ctx, "GET", book.GatewayURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		gatewayResp, err := client.Do(req)
		if err != nil {
			return tmpDir, "", "", err
		}
		defer gatewayResp.Body.Close()

		gatewayDoc, err := goquery.NewDocumentFromReader(gatewayResp.Body)
		if err != nil {
			return tmpDir, "", "", fmt.Errorf("failed to parse gateway HTML")
		}

		gatewayDoc.Find("a").Each(func(i int, s *goquery.Selection) {
			if strings.ToUpper(strings.TrimSpace(s.Text())) == "GET" {
				if href, exists := s.Attr("href"); exists {
					finalDownloadURL = href
				}
			}
		})

		if finalDownloadURL == "" {
			return tmpDir, "", "", fmt.Errorf("could not locate the 'GET' button")
		}
		if !strings.HasPrefix(finalDownloadURL, "http") {
			if !strings.HasPrefix(finalDownloadURL, "/") {
				finalDownloadURL = "/" + finalDownloadURL
			}
			finalDownloadURL = "https://libgen.li" + finalDownloadURL
		}
		editMessage(bot, chatID, msgID, "📥 *Requesting File Stream...*", taskID)
	}

	fileReq, _ := http.NewRequestWithContext(ctx, "GET", finalDownloadURL, nil)
	fileReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	if book.GatewayURL != "" {
		fileReq.Header.Set("Referer", book.GatewayURL)
	}

	fileResp, err := client.Do(fileReq)
	if err != nil {
		return tmpDir, "", "", err
	}
	defer fileResp.Body.Close()

	if fileResp.StatusCode != 200 {
		return tmpDir, "", "", fmt.Errorf("server rejected request: Status %d", fileResp.StatusCode)
	}
	if fileResp.ContentLength > 2000*1024*1024 {
		return tmpDir, "", "", fmt.Errorf("file size exceeds 2GB limit")
	}

	parsedURL, _ := url.Parse(finalDownloadURL)
	ext := filepath.Ext(parsedURL.Path)

	contentDisposition := fileResp.Header.Get("Content-Disposition")
	if contentDisposition != "" {
		_, params, err := mime.ParseMediaType(contentDisposition)
		if err == nil {
			if filename, ok := params["filename"]; ok {
				if extractedExt := filepath.Ext(filename); extractedExt != "" {
					ext = extractedExt
				}
			}
		}
	} else if ext == "" {
		contentType := fileResp.Header.Get("Content-Type")
		if strings.Contains(contentType, "pdf") {
			ext = ".pdf"
		} else if strings.Contains(contentType, "epub") {
			ext = ".epub"
		} else if strings.Contains(contentType, "mobi") {
			ext = ".mobi"
		} else if strings.Contains(contentType, "mp4") {
			ext = ".mp4"
		} else if strings.Contains(contentType, "zip") {
			ext = ".zip"
		}
	}
	if ext == "" {
		ext = ".file"
	}

	safeTitle := sanitizeFilename(book.Title)
	bookPath := filepath.Join(tmpDir, safeTitle+ext)
	bookFile, err := os.Create(bookPath)
	if err != nil {
		return tmpDir, "", "", err
	}

	progress := &DownloadProgress{
		Ctx:        ctx,
		TaskID:     taskID,
		TotalSize:  fileResp.ContentLength,
		Bot:        bot,
		ChatID:     chatID,
		MsgID:      msgID,
		LastUpdate: time.Now(),
	}

	_, err = io.Copy(bookFile, io.TeeReader(fileResp.Body, progress))
	if err != nil {
		return tmpDir, "", "", err
	}

	coverPath := ""
	if book.CoverURL != "" {
		imgReq, _ := http.NewRequestWithContext(ctx, "GET", book.CoverURL, nil)
		imgReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		coverResp, err := client.Do(imgReq)

		if err == nil && coverResp.StatusCode == 200 {
			defer coverResp.Body.Close()
			coverPath = filepath.Join(tmpDir, "cover.jpg")
			coverFile, err := os.Create(coverPath)
			if err == nil {
				defer coverFile.Close()
				io.Copy(coverFile, coverResp.Body)
			}
		}
	}

	return tmpDir, bookPath, coverPath, nil
}

// ---------------------------------------------------------
// TEXT HELPERS
// ---------------------------------------------------------
func extractDynamicInterfaceField(m map[string]interface{}, key string) string {
	val, ok := m[key]
	if !ok || val == nil {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	if arr, ok := val.([]interface{}); ok && len(arr) > 0 {
		if firstStr, ok := arr[0].(string); ok {
			return firstStr
		}
	}
	return ""
}

func stripHTML(text string) string {
	re := regexp.MustCompile(`<[^>]*>`)
	return re.ReplaceAllString(text, "")
}

func cleanText(text string) string {
	text = strings.TrimSpace(text)
	text = strings.ReplaceAll(text, "\n", " ")
	text = strings.ReplaceAll(text, "\t", "")
	for strings.Contains(text, "  ") {
		text = strings.ReplaceAll(text, "  ", " ")
	}
	return text
}

func editMessage(bot *tgbotapi.BotAPI, chatID int64, msgID int, newText string, taskID string) {
	edit := tgbotapi.NewEditMessageText(chatID, msgID, newText)
	edit.ParseMode = "Markdown"

	if taskID != "" {
		edit.ReplyMarkup = cancelKeyboard(taskID)
	}

	_, _ = bot.Send(edit)
}

func removeClickedButton(bot *tgbotapi.BotAPI, query *tgbotapi.CallbackQuery) {
	if query.Message == nil || query.Message.ReplyMarkup == nil {
		return
	}

	var newKeyboard [][]tgbotapi.InlineKeyboardButton

	// Loop through existing rows
	for _, row := range query.Message.ReplyMarkup.InlineKeyboard {
		var newRow []tgbotapi.InlineKeyboardButton

		// Loop through buttons in the row
		for _, btn := range row {
			// If the button's data matches what they just clicked, skip it!
			if btn.CallbackData != nil && *btn.CallbackData == query.Data {
				continue
			}
			newRow = append(newRow, btn)
		}

		// Only keep the row if it still has buttons left
		if len(newRow) > 0 {
			newKeyboard = append(newKeyboard, newRow)
		}
	}

	// Update the message with the smaller keyboard
	edit := tgbotapi.NewEditMessageReplyMarkup(
		query.Message.Chat.ID,
		query.Message.MessageID,
		tgbotapi.InlineKeyboardMarkup{InlineKeyboard: newKeyboard},
	)
	bot.Send(edit)
}

// ---------------------------------------------------------
// MEMORY MANAGEMENT
// ---------------------------------------------------------
func startSessionCleaner() {
	// Run this check every 1 hour
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		sessionMutex.Lock()
		now := time.Now()
		for id, session := range archiveSessions {
			// If the session is older than 2 hours, delete it to free RAM
			if now.Sub(session.CreatedAt) > 2*time.Hour {
				delete(archiveSessions, id)
			}
		}
		sessionMutex.Unlock()
	}
}

// ---------------------------------------------------------
// PDF COMPRESSION HELPER
// ---------------------------------------------------------
func compressPDF(inputPath, outputPath string) error {
	cmd := exec.Command("gs",
		"-sDEVICE=pdfwrite",
		"-dCompatibilityLevel=1.4",
		"-dPDFSETTINGS=/ebook",
		"-dNOPAUSE",
		"-dQUIET",
		"-dBATCH",
		"-sOutputFile="+outputPath,
		inputPath,
	)
	return cmd.Run()
}
