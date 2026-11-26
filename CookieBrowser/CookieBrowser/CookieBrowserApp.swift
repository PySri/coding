import SwiftUI

@main
struct CookieBrowserApp: App {
    var body: some Scene {
        WindowGroup { RootView() }
    }
}

struct RootView: View {
    @StateObject private var browser = BrowserModel()
    @StateObject private var manager = CookieManager()
    @AppStorage("autoClean") private var autoClean = true
    @AppStorage("blockTrackers") private var blockTrackers = true
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        TabView {
            BrowserView(browser: browser)
                .tabItem { Label("Browser", systemImage: "globe") }
            CookiesView(manager: manager)
                .tabItem { Label("Cookies", systemImage: "list.bullet.rectangle") }
                .badge(manager.trackerCount)
        }
        .task {
            await browser.setBlocking(blockTrackers)
            browser.onPageLoaded = {
                Task {
                    if autoClean { await manager.deleteTrackers() } else { await manager.refresh() }
                }
            }
            if browser.webView.url == nil { browser.load("https://duckduckgo.com") }
        }
        .onChange(of: blockTrackers) { _, on in Task { await browser.setBlocking(on) } }
        .onChange(of: scenePhase) { _, phase in
            if phase == .background, autoClean { Task { await manager.deleteTrackers() } }
        }
    }
}
