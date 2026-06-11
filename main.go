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
	"path/filepath"
	"regexp"
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
	botToken := os.Getenv("BOT_TOKEN")
	if botToken == "" {
		log.Panic("BOT_TOKEN environment variable is not set")
	}

	bot, err := tgbotapi.NewBotAPIWithAPIEndpoint(botToken, "http://telegram-bot-api:8081/bot%s/%s")
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

	if strings.HasPrefix(query.Data, "cancel_") {
		taskID := strings.TrimPrefix(query.Data, "cancel_")

		taskMutex.Lock()
		if cancelFunc, exists := activeTasks[taskID]; exists {
			cancelFunc()
			delete(activeTasks, taskID)
		}
		taskMutex.Unlock()
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
	statusMsg := tgbotapi.NewMessage(chatID, fmt.Sprintf("🔍 Analyzing Link #%d...", itemNum))
	sentStatus, err := bot.Send(statusMsg)
	if err != nil {
		return
	}
	msgID := sentStatus.MessageID

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

	var book *BookData

	// UNIVERSAL ROUTER
	if strings.Contains(linkURL, "archive.org/details/") {
		book, err = getArchiveOrgData(linkURL)
	} else if strings.Contains(linkURL, "libgen.li") && !strings.HasSuffix(linkURL, ".pdf") && !strings.HasSuffix(linkURL, ".epub") {
		book, err = scrapeLibgen(linkURL)
	} else {
		// ULTIMATE FALLBACK: Treat as a Direct File Link
		parsedURL, _ := url.Parse(linkURL)
		filename := filepath.Base(parsedURL.Path)
		if filename == "." || filename == "/" || filename == "" {
			filename = fmt.Sprintf("Downloaded_File_%d", itemNum)
		}

		// If it's a URL-encoded string like %20, clean it up for the title
		if unescaped, err := url.PathUnescape(filename); err == nil {
			filename = unescaped
		}

		book = &BookData{Title: sanitizeFilename(filename), DirectURL: linkURL}
	}

	if err != nil {
		editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Error processing Link #%d: %v", itemNum, err), "")
		return
	}

	tmpDir, bookPath, coverPath, err := downloadFiles(ctx, bot, chatID, msgID, taskID, book)
	if err != nil {
		if ctx.Err() != nil {
			editMessage(bot, chatID, msgID, "🚫 *Task Cancelled by User.*", "")
		} else {
			editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Download #%d failed: %v", itemNum, err), "")
		}
		if tmpDir != "" {
			os.RemoveAll(tmpDir)
		}
		return
	}
	defer os.RemoveAll(tmpDir)

	editMessage(bot, chatID, msgID, fmt.Sprintf("📤 Formatting upload for item #%d...", itemNum), taskID)

	var caption string
	if book.Author != "" || book.Publisher != "" {
		caption = fmt.Sprintf("📖 *Title:* %s\n👤 *Author:* %s\n📅 *Year:* %s\n🏢 *Publisher:* %s",
			book.Title, book.Author, book.Year, book.Publisher)
		if book.Description != "" {
			caption += fmt.Sprintf("\n\n*Description:*\n%s", book.Description)
		}
	} else {
		caption = fmt.Sprintf("📄 *File:* %s", filepath.Base(bookPath))
	}
	if len(caption) > 1000 {
		caption = caption[:997] + "..."
	}

	if coverPath != "" {
		photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(coverPath))
		photo.Caption = caption
		photo.ParseMode = "Markdown"
		bot.Send(photo)
	}

	file, err := os.Open(bookPath)
	if err != nil {
		editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Failed to read local file #%d: %v", itemNum, err), "")
		return
	}
	defer file.Close()

	stat, _ := file.Stat()
	uploadTracker := &UploadProgress{
		Ctx:        ctx,
		TaskID:     taskID,
		Reader:     file,
		TotalSize:  stat.Size(),
		Bot:        bot,
		ChatID:     chatID,
		MsgID:      msgID,
		LastUpdate: time.Now(),
	}

	doc := tgbotapi.NewDocument(chatID, tgbotapi.FileReader{
		Name:   filepath.Base(bookPath),
		Reader: uploadTracker,
	})

	if coverPath == "" {
		doc.Caption = caption
		doc.ParseMode = "Markdown"
	}

	_, err = bot.Send(doc)
	if err != nil {
		if ctx.Err() != nil {
			editMessage(bot, chatID, msgID, "🚫 *Upload Cancelled by User.*", "")
		} else {
			editMessage(bot, chatID, msgID, fmt.Sprintf("❌ Telegram upload #%d failed: %v", itemNum, err), "")
		}
		return
	}

	editMessage(bot, chatID, msgID, fmt.Sprintf("✅ Upload #%d Complete!", itemNum), "")
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

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	clean := re.ReplaceAllString(name, "")
	if len(clean) > 80 {
		clean = clean[:80]
	}
	return strings.TrimSpace(clean)
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
