import SwiftUI
import WebKit

/// Holds the WKWebView so SwiftUI controls (address bar, back, forward) can drive it.
@MainActor
final class BrowserModel: NSObject, ObservableObject, WKNavigationDelegate {
    @Published var address = ""
    @Published var canGoBack = false
    @Published var canGoForward = false
    @Published var isLoading = false

    let webView: WKWebView
    var onPageLoaded: (() -> Void)?
    private var observers: [NSKeyValueObservation] = []

    override init() {
        let config = WKWebViewConfiguration()
        config.websiteDataStore = .default()
        webView = WKWebView(frame: .zero, configuration: config)
        webView.allowsBackForwardNavigationGestures = true
        super.init()
        webView.navigationDelegate = self
        observers = [
            webView.observe(\.canGoBack) { [weak self] wv, _ in Task { @MainActor in self?.canGoBack = wv.canGoBack } },
            webView.observe(\.canGoForward) { [weak self] wv, _ in Task { @MainActor in self?.canGoForward = wv.canGoForward } },
            webView.observe(\.isLoading) { [weak self] wv, _ in Task { @MainActor in self?.isLoading = wv.isLoading } },
            webView.observe(\.url) { [weak self] wv, _ in
                Task { @MainActor in if let u = wv.url?.absoluteString { self?.address = u } }
            },
        ]
    }

    func setBlocking(_ enabled: Bool) async {
        let controller = webView.configuration.userContentController
        controller.removeAllContentRuleLists()
        if enabled, let list = await ContentBlocker.ruleList() { controller.add(list) }
    }

    func load(_ text: String) {
        guard let url = Self.url(from: text) else { return }
        webView.load(URLRequest(url: url))
    }

    /// Turns typed text into a URL: adds https:// or falls back to a DuckDuckGo search.
    nonisolated static func url(from text: String) -> URL? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }
        if trimmed.contains("://"), let u = URL(string: trimmed) { return u }
        if trimmed.contains("."), !trimmed.contains(" "), let u = URL(string: "https://" + trimmed) { return u }
        var c = URLComponents(string: "https://duckduckgo.com/")!
        c.queryItems = [URLQueryItem(name: "q", value: trimmed)]
        return c.url
    }

    nonisolated func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        Task { @MainActor in self.onPageLoaded?() }
    }
}

struct WebViewContainer: UIViewRepresentable {
    let webView: WKWebView
    func makeUIView(context: Context) -> WKWebView { webView }
    func updateUIView(_ uiView: WKWebView, context: Context) {}
}

struct BrowserView: View {
    @ObservedObject var browser: BrowserModel
    @FocusState private var editing: Bool

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 8) {
                TextField("Search or enter website", text: $browser.address)
                    .textFieldStyle(.roundedBorder)
                    .keyboardType(.webSearch)
                    .textInputAutocapitalization(.never)
                    .autocorrectionDisabled()
                    .submitLabel(.go)
                    .focused($editing)
                    .onSubmit { browser.load(browser.address) }
                    .accessibilityIdentifier("addressBar")
                if browser.isLoading { ProgressView() }
            }
            .padding(.horizontal)
            .padding(.vertical, 8)

            WebViewContainer(webView: browser.webView)

            HStack {
                Button { browser.webView.goBack() } label: { Image(systemName: "chevron.left") }
                    .disabled(!browser.canGoBack)
                Spacer()
                Button { browser.webView.goForward() } label: { Image(systemName: "chevron.right") }
                    .disabled(!browser.canGoForward)
                Spacer()
                Button { browser.webView.reload() } label: { Image(systemName: "arrow.clockwise") }
            }
            .font(.title3)
            .padding(.horizontal, 40)
            .padding(.vertical, 10)
            .background(.bar)
        }
    }
}
