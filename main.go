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
	Size        int64 // Used for queue management
}

// ---------------------------------------------------------
// ARCHIVE.ORG SESSION CACHE (For Interactive Menu)
// ---------------------------------------------------------
var (
	archiveSessions = make(map[string]*ArchiveSession)
	sessionMutex    sync.Mutex
	sessionCounter  uint64 // Prevents ID collisions on rapid multi-links
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
// USER STATE CACHE (For Renaming)
// ---------------------------------------------------------
type UserState struct {
	ChatID         int64
	State          string // "CONFIRM_OR_RENAME" or "AWAITING_RENAME"
	PendingBook    *BookData
	ShouldCompress bool
	SessionID      string // NEW: Links to the Archive session
	CreatedAt      time.Time
}

var (
	userStates     = make(map[int]*UserState) // Keyed by prompt MsgID to allow multi-prompts
	userStateMutex sync.Mutex
)

// ---------------------------------------------------------
// DOWNLOAD QUEUE DISPATCHER
// ---------------------------------------------------------
type DownloadJob struct {
	Bot            *tgbotapi.BotAPI
	ChatID         int64
	MsgID          int
	Book           *BookData
	ShouldCompress bool
}

var (
	jobQueue    = make(chan *DownloadJob, 200)
	activeJobs  int
	activeBytes int64
	jobMutex    sync.Mutex
	jobCond     = sync.NewCond(&jobMutex)
	MAX_JOBS    = 3
	MAX_BYTES   = int64(800 * 1024 * 1024)
)

func startDispatcher() {
	for job := range jobQueue {
		jobMutex.Lock()

		// Wait if we have 3 active jobs, OR if adding this job breaks the 800MB limit.
		// (If activeJobs == 0, we bypass the byte limit to prevent deadlocks on massive files)
		for activeJobs >= MAX_JOBS || (activeJobs > 0 && activeBytes+job.Book.Size > MAX_BYTES) {
			jobCond.Wait()
		}

		activeJobs++
		activeBytes += job.Book.Size
		jobMutex.Unlock()

		editMessage(job.Bot, job.ChatID, job.MsgID, "📥 *Starting download...*", "")

		// Run the actual download in a separate worker routine
		go func(j *DownloadJob) {
			runDownloadPipeline(j.Bot, j.ChatID, j.MsgID, j.Book, j.ShouldCompress)

			// When finished, release resources and wake up the dispatcher
			jobMutex.Lock()
			activeJobs--
			activeBytes -= j.Book.Size
			if activeBytes < 0 {
				activeBytes = 0
			}
			jobCond.Broadcast()
			jobMutex.Unlock()
		}(job)
	}
}

// ---------------------------------------------------------
// TEXT HELPERS & CANCEL LOGIC
// ---------------------------------------------------------

func sanitizeFilename(name string) string {
	re := regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)
	clean := re.ReplaceAllString(name, "")

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

// ---------------------------------------------------------
// URL UN-SHORTENER
// ---------------------------------------------------------
func resolveShortLink(linkURL string) string {
	client := &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}

	// First try a lightweight HEAD request
	req, err := http.NewRequest("HEAD", linkURL, nil)
	if err != nil {
		return linkURL
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := client.Do(req)
	if err != nil {
		// If HEAD fails, fallback to a standard GET request
		req, _ = http.NewRequest("GET", linkURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
		resp, err = client.Do(req)
		if err != nil {
			return linkURL // Return original if totally unreachable
		}
	}
	defer resp.Body.Close()

	return resp.Request.URL.String() // Return the final, unfurled URL!
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
	go startDispatcher() // Start the Queue engine

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

	if strings.HasPrefix(query.Data, "ar|") {
		go processArchiveCallback(bot, query)
		return
	}

	if strings.HasPrefix(query.Data, "dl|") {
		go processDownloadCallback(bot, query)
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
	go removeClickedButton(bot, query)

	if action == "ren" {
		userStateMutex.Lock()
		userStates[query.Message.MessageID] = &UserState{
			ChatID:    chatID,
			State:     "AWAITING_ARCHIVE_RENAME",
			SessionID: sessionID,
			CreatedAt: time.Now(),
		}
		userStateMutex.Unlock()

		msg := tgbotapi.NewMessage(chatID, "✏️ *Send me the new filename for this book:*\n*(Please do not include extensions like .pdf)*")
		msg.ParseMode = "Markdown"
		bot.Send(msg)
		return
	}

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

		pathParts := strings.Split(f.Name, "/")
		for i, p := range pathParts {
			pathParts[i] = url.PathEscape(p)
		}

		// Parse size to inform the Dispatcher limit
		sizeBytes, _ := strconv.ParseInt(f.Size, 10, 64)

		book := &BookData{
			Title:     session.Title,
			DirectURL: "https://archive.org/download/" + session.ItemID + "/" + strings.Join(pathParts, "/"),
			Size:      sizeBytes,
		}

		shouldCompress := action == "comp"
		// --- CHANGED: Skip the double-prompt, go straight to the queue! ---
		statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ *Queued for download...*\nWaiting for available slot."))
		jobQueue <- &DownloadJob{Bot: bot, ChatID: chatID, MsgID: statusMsg.MessageID, Book: book, ShouldCompress: shouldCompress}
	}
}

func handleUserMessage(bot *tgbotapi.BotAPI, msg *tgbotapi.Message) {
	chatID := msg.Chat.ID
	userText := strings.TrimSpace(msg.Text)

	// --- INTERCEPT TEXT IF WAITING FOR A RENAME ---
	var activeState *UserState
	var activeMsgID int

	userStateMutex.Lock()
	for id, state := range userStates {
		if state.ChatID == chatID && (state.State == "AWAITING_ARCHIVE_RENAME" || state.State == "AWAITING_LIBGEN_RENAME") {
			activeState = state
			activeMsgID = id
			break
		}
	}
	userStateMutex.Unlock()

	if activeState != nil {
		newName := sanitizeFilename(userText)
		if newName == "" {
			bot.Send(tgbotapi.NewMessage(chatID, "❌ Invalid filename. Try typing it again:"))
			return
		}

		userStateMutex.Lock()
		delete(userStates, activeMsgID)
		userStateMutex.Unlock()

		addReactions(bot, chatID, msg.MessageID, "🕊")

		// If renaming an Archive menu item:
		if activeState.State == "AWAITING_ARCHIVE_RENAME" {
			sessionMutex.Lock()
			if sess, ok := archiveSessions[activeState.SessionID]; ok {
				sess.Title = newName
			}
			sessionMutex.Unlock()

			bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Filename updated to `%s`!\n👉 *Now click a download button on the menu above.*", newName)))
			return
		}

		// If renaming a Libgen/Direct link item:
		if activeState.State == "AWAITING_LIBGEN_RENAME" {
			activeState.PendingBook.Title = newName
			book := activeState.PendingBook
			comp := activeState.ShouldCompress

			bot.Send(tgbotapi.NewMessage(chatID, fmt.Sprintf("✅ Filename updated to `%s`", newName)))
			statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ *Queued for download...*\nWaiting for available slot."))
			jobQueue <- &DownloadJob{Bot: bot, ChatID: chatID, MsgID: statusMsg.MessageID, Book: book, ShouldCompress: comp}
			return
		}
	}

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

	// --- NEW: EXPAND SHORTENED LINKS ---
	for i, u := range urls {
		urls[i] = resolveShortLink(u)
	}

	// --- SCAN LINKS FOR REACTIONS ---
	var linkReactions []string
	hasArchive, hasLibgen := false, false

	for _, u := range urls {
		if strings.Contains(u, "archive.org") && !hasArchive {
			linkReactions = append(linkReactions, "🤝")
			hasArchive = true
		}
		if strings.Contains(u, "libgen") && !hasLibgen {
			linkReactions = append(linkReactions, "⚡")
			hasLibgen = true
		}
	}
	addReactions(bot, chatID, msg.MessageID, linkReactions...)

	for i, u := range urls {
		go processSingleLink(bot, chatID, u, i+1)
	}
}

func processSingleLink(bot *tgbotapi.BotAPI, chatID int64, linkURL string, itemNum int) {
	if strings.Contains(linkURL, "archive.org/details/") {
		handleArchiveMenu(bot, chatID, linkURL)
		return
	}

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
		book = &BookData{Title: sanitizeFilename(filename), DirectURL: linkURL, Size: 0}
	}

	if err != nil {
		editMessage(bot, chatID, sentStatus.MessageID, fmt.Sprintf("❌ Error processing Link #%d: %v", itemNum, err), "")
		return
	}

	bot.Request(tgbotapi.NewDeleteMessage(chatID, sentStatus.MessageID))
	promptForRename(bot, chatID, book, false)
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

	bestCoverURL := "https://archive.org/services/img/" + id
	probeClient := &http.Client{Timeout: 5 * time.Second}
	highResURLs := []string{
		"https://archive.org/download/" + id + "/page/cover_w1000.jpg",
		"https://archive.org/download/" + id + "/page/n0_w1000.jpg",
		"https://archive.org/download/" + id + "/page/n1_w1000.jpg",
	}

	for _, testURL := range highResURLs {
		req, _ := http.NewRequest("HEAD", testURL, nil)
		resp, err := probeClient.Do(req)
		if err == nil && resp.StatusCode == 200 {
			bestCoverURL = testURL
			break
		}
	}

	// Atomic counter for unique session IDs
	sessionMutex.Lock()
	sessionCounter++
	sessionID := fmt.Sprintf("ar_%d", sessionCounter)
	sessionMutex.Unlock()

	session := &ArchiveSession{
		ItemID:    id,
		Title:     cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "title")),
		Author:    cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "creator")),
		Publisher: cleanText(extractDynamicInterfaceField(archiveRes.Metadata, "publisher")),
		CoverURL:  bestCoverURL,
		Files:     archiveRes.Files,
		CreatedAt: time.Now(),
	}
	if session.Title == "" {
		session.Title = "Archive_Item_" + id
	}

	sessionMutex.Lock()
	archiveSessions[sessionID] = session
	sessionMutex.Unlock()

	var keyboard [][]tgbotapi.InlineKeyboardButton
	keyboard = append(keyboard, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("🖼 Cover + Metadata", "ar|meta|"+sessionID),
		tgbotapi.NewInlineKeyboardButtonData("✏️ Rename", "ar|ren|"+sessionID), // <-- NEW BUTTON
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
		row := []tgbotapi.InlineKeyboardButton{
			tgbotapi.NewInlineKeyboardButtonData(btnText, cbData),
		}

		if sizeMB > 200 && strings.HasSuffix(lowerName, ".pdf") {
			compCbData := fmt.Sprintf("ar|comp|%s|%d", sessionID, i)
			row = append(row, tgbotapi.NewInlineKeyboardButtonData("🗜 Compress", compCbData))
		}
		keyboard = append(keyboard, tgbotapi.NewInlineKeyboardRow(row...))
	}

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

	if shouldCompress && filepath.Ext(bookPath) == ".pdf" {
		editMessage(bot, chatID, msgID, "🗜 *Compressing PDF...*\nThis might take a minute depending on the file complexity.", taskID)
		compressedPath := filepath.Join(tmpDir, "compressed_"+filepath.Base(bookPath))
		err := compressPDF(bookPath, compressedPath)
		if err == nil {
			bookPath = compressedPath
		} else {
			editMessage(bot, chatID, msgID, "⚠️ Compression failed! Uploading original size instead...", taskID)
			time.Sleep(2 * time.Second)
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

	book := &BookData{Size: 0}
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
			src = strings.ReplaceAll(src, "_small/", "")
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

func downloadFiles(ctx context.Context, bot *tgbotapi.BotAPI, chatID int64, msgID int, taskID string, book *BookData) (string, string, string, error) {
	tmpDir, err := os.MkdirTemp("", "library_*")
	if err != nil {
		return "", "", "", err
	}

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Timeout: 180 * time.Second, Jar: jar}

	var finalDownloadURL string

	if book.DirectURL != "" {
		finalDownloadURL = book.DirectURL
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
		Ctx: ctx, TaskID: taskID, TotalSize: fileResp.ContentLength,
		Bot: bot, ChatID: chatID, MsgID: msgID, LastUpdate: time.Now(),
	}
	_, err = io.Copy(bookFile, io.TeeReader(fileResp.Body, progress))
	if err != nil {
		return tmpDir, "", "", err
	}

	return tmpDir, bookPath, "", nil
}

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

func stripHTML(text string) string { return regexp.MustCompile(`<[^>]*>`).ReplaceAllString(text, "") }

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
	for _, row := range query.Message.ReplyMarkup.InlineKeyboard {
		var newRow []tgbotapi.InlineKeyboardButton
		for _, btn := range row {
			if btn.CallbackData != nil && *btn.CallbackData == query.Data {
				continue
			}
			newRow = append(newRow, btn)
		}
		if len(newRow) > 0 {
			newKeyboard = append(newKeyboard, newRow)
		}
	}
	edit := tgbotapi.NewEditMessageReplyMarkup(query.Message.Chat.ID, query.Message.MessageID, tgbotapi.InlineKeyboardMarkup{InlineKeyboard: newKeyboard})
	bot.Send(edit)
}

func addReactions(bot *tgbotapi.BotAPI, chatID int64, msgID int, emojis ...string) {
	if len(emojis) == 0 {
		return
	}
	var reactions []map[string]string
	for _, e := range emojis {
		reactions = append(reactions, map[string]string{"type": "emoji", "emoji": e})
	}
	params := tgbotapi.Params{
		"chat_id":    strconv.FormatInt(chatID, 10),
		"message_id": strconv.Itoa(msgID),
		"is_big":     "true",
	}
	reactionJSON, _ := json.Marshal(reactions)
	params["reaction"] = string(reactionJSON)
	bot.MakeRequest("setMessageReaction", params)
}

func startSessionCleaner() {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		now := time.Now()
		sessionMutex.Lock()
		for id, session := range archiveSessions {
			if now.Sub(session.CreatedAt) > 2*time.Hour {
				delete(archiveSessions, id)
			}
		}
		sessionMutex.Unlock()

		userStateMutex.Lock()
		for id, state := range userStates {
			if now.Sub(state.CreatedAt) > 1*time.Hour {
				delete(userStates, id)
			}
		}
		userStateMutex.Unlock()
	}
}

func compressPDF(inputPath, outputPath string) error {
	cmd := exec.Command("gs", "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4", "-dPDFSETTINGS=/ebook", "-dNOPAUSE", "-dQUIET", "-dBATCH", "-sOutputFile="+outputPath, inputPath)
	return cmd.Run()
}

func promptForRename(bot *tgbotapi.BotAPI, chatID int64, book *BookData, shouldCompress bool) {
	defaultName := sanitizeFilename(book.Title)

	sizeText := ""
	if book.Size > 0 {
		sizeText = fmt.Sprintf("\nSize: %.2f MB", float64(book.Size)/(1024*1024))
	}

	text := fmt.Sprintf("📖 *%s*%s\n\nChoose an option below:", defaultName, sizeText)

	kb := tgbotapi.NewInlineKeyboardMarkup(
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📥 Download", "dl|confirm"), // Re-worded to feel like a standard menu
			tgbotapi.NewInlineKeyboardButtonData("✏️ Rename", "dl|rename"),
		),
		tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("❌ Cancel", "dl|cancel"),
		),
	)

	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "Markdown"
	msg.ReplyMarkup = kb
	sentMsg, err := bot.Send(msg)
	if err != nil {
		return
	}

	userStateMutex.Lock()
	userStates[sentMsg.MessageID] = &UserState{
		ChatID:         chatID,
		State:          "CONFIRM_OR_RENAME",
		PendingBook:    book,
		ShouldCompress: shouldCompress,
		CreatedAt:      time.Now(),
	}
	userStateMutex.Unlock()
}

func processDownloadCallback(bot *tgbotapi.BotAPI, query *tgbotapi.CallbackQuery) {
	action := strings.TrimPrefix(query.Data, "dl|")
	chatID := query.Message.Chat.ID
	msgID := query.Message.MessageID

	userStateMutex.Lock()
	state, exists := userStates[msgID]
	userStateMutex.Unlock()

	if !exists || state.PendingBook == nil {
		bot.Send(tgbotapi.NewMessage(chatID, "❌ Session expired. Please send the link again."))
		bot.Request(tgbotapi.NewDeleteMessage(chatID, msgID))
		return
	}

	if action == "cancel" {
		bot.Request(tgbotapi.NewDeleteMessage(chatID, msgID))
		userStateMutex.Lock()
		delete(userStates, msgID)
		userStateMutex.Unlock()
		return
	}

	if action == "confirm" {
		bot.Request(tgbotapi.NewDeleteMessage(chatID, msgID))
		book := state.PendingBook
		comp := state.ShouldCompress

		userStateMutex.Lock()
		delete(userStates, msgID)
		userStateMutex.Unlock()

		statusMsg, _ := bot.Send(tgbotapi.NewMessage(chatID, "⏳ *Queued for download...*\nWaiting for available slot."))
		jobQueue <- &DownloadJob{Bot: bot, ChatID: chatID, MsgID: statusMsg.MessageID, Book: book, ShouldCompress: comp}
		return
	}

	if action == "rename" {
		userStateMutex.Lock()
		state.State = "AWAITING_LIBGEN_RENAME"
		userStateMutex.Unlock()

		edit := tgbotapi.NewEditMessageText(chatID, msgID, "✏️ *Send me the new filename:*\n*(Please do not include extensions like .pdf)*")
		edit.ParseMode = "Markdown"
		bot.Send(edit)
	}
}
