import Foundation
import WebKit

struct CookieItem: Identifiable, Hashable {
    let cookie: HTTPCookie
    let category: CookieCategory

    var id: String { "\(cookie.domain)|\(cookie.path)|\(cookie.name)" }
    var site: String { TrackerRules.site(for: cookie.domain) }
    var host: String { cookie.domain.hasPrefix(".") ? String(cookie.domain.dropFirst()) : cookie.domain }

    init(_ cookie: HTTPCookie) {
        self.cookie = cookie
        self.category = TrackerRules.category(host: cookie.domain, name: cookie.name)
    }
}

struct SiteGroup: Identifiable {
    let site: String
    let cookies: [CookieItem]
    var id: String { site }
    var trackerCount: Int { cookies.filter { $0.category.isTracker }.count }
}

/// Reads and deletes the cookies of the app's own browser (its WebKit data store).
@MainActor
final class CookieManager: NSObject, ObservableObject, WKHTTPCookieStoreObserver {
    @Published private(set) var cookies: [CookieItem] = []
    @Published private(set) var lastCleanup: String?

    private let store = WKWebsiteDataStore.default()
    private var cookieStore: WKHTTPCookieStore { store.httpCookieStore }
    private var pendingRefresh: Task<Void, Never>?

    override init() {
        super.init()
        cookieStore.add(self)
    }

    /// Sites set cookies in bursts; refresh at most every half second so the list stays live.
    nonisolated func cookiesDidChange(in cookieStore: WKHTTPCookieStore) {
        Task { @MainActor in
            guard self.pendingRefresh == nil else { return }
            self.pendingRefresh = Task {
                try? await Task.sleep(for: .milliseconds(500))
                await self.refresh()
                self.pendingRefresh = nil
            }
        }
    }

    var sites: [SiteGroup] {
        Dictionary(grouping: cookies, by: \.site)
            .map { SiteGroup(site: $0.key, cookies: $0.value.sorted { $0.cookie.name < $1.cookie.name }) }
            .sorted { $0.cookies.count != $1.cookies.count ? $0.cookies.count > $1.cookies.count : $0.site < $1.site }
    }

    func count(_ category: CookieCategory) -> Int { cookies.filter { $0.category == category }.count }
    var trackerCount: Int { cookies.filter { $0.category.isTracker }.count }

    func refresh() async {
        let all = await cookieStore.allCookies()
        cookies = all.map(CookieItem.init)
    }

    func delete(_ items: [CookieItem]) async {
        for item in items { await cookieStore.deleteCookie(item.cookie) }
        await refresh()
    }

    @discardableResult
    func deleteTrackers() async -> Int {
        await refresh()
        let trackers = cookies.filter { $0.category.isTracker }
        await delete(trackers)
        let stamp = Date().formatted(date: .abbreviated, time: .shortened)
        lastCleanup = "\(stamp): deleted \(trackers.count) tracker cookie\(trackers.count == 1 ? "" : "s")"
        return trackers.count
    }

    func deleteSite(_ site: String) async {
        await delete(cookies.filter { $0.site == site })
    }

    /// Deletes everything: cookies plus cached site data (local storage, caches, etc.).
    func deleteAllWebsiteData() async {
        await store.removeData(ofTypes: WKWebsiteDataStore.allWebsiteDataTypes(), modifiedSince: .distantPast)
        await refresh()
    }
}
