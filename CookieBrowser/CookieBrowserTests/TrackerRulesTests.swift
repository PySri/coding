import XCTest
@testable import CookieBrowser

final class TrackerRulesTests: XCTestCase {
    func testWildcard() {
        XCTAssertTrue(TrackerRules.wildcard("_ga_ABC123", "_ga_*"))
        XCTAssertTrue(TrackerRules.wildcard("_ga", "_ga"))
        XCTAssertFalse(TrackerRules.wildcard("_gax", "_ga"))
        XCTAssertTrue(TrackerRules.wildcard("_sp_id.e48a", "_sp_id.*"))
        XCTAssertFalse(TrackerRules.wildcard("sp_id", "_sp_id.*"))
    }

    func testCategories() {
        XCTAssertEqual(TrackerRules.category(host: ".doubleclick.net", name: "anything"), .advertising)
        XCTAssertEqual(TrackerRules.category(host: ".atomtickets.com", name: "_gcl_au"), .advertising)
        XCTAssertEqual(TrackerRules.category(host: ".wsj.com", name: "_pubcid"), .advertising)
        XCTAssertEqual(TrackerRules.category(host: ".mercurynews.com", name: "_ga_13TLJGDS7P"), .analytics)
        XCTAssertEqual(TrackerRules.category(host: ".wsj.com", name: "TR"), .analytics)
        XCTAssertEqual(TrackerRules.category(host: ".example.com", name: "TR"), .other)
        XCTAssertEqual(TrackerRules.category(host: ".google.com", name: "SID"), .signIn)
        XCTAssertEqual(TrackerRules.category(host: "claude.ai", name: "sessionKey"), .signIn)
    }

    func testKeepList() {
        XCTAssertFalse(TrackerRules.category(host: ".youtube.com", name: "VISITOR_INFO1_LIVE").isTracker)
        XCTAssertFalse(TrackerRules.category(host: ".linkedin.com", name: "bcookie").isTracker)
        XCTAssertFalse(TrackerRules.category(host: ".apple.com", name: "dssid2").isTracker)
        XCTAssertTrue(TrackerRules.category(host: ".youtube.com", name: "YSC").isTracker)
    }

    func testSiteGrouping() {
        XCTAssertEqual(TrackerRules.site(for: "mail.google.com"), "google.com")
        XCTAssertEqual(TrackerRules.site(for: ".www.mercurynews.com"), "mercurynews.com")
        XCTAssertEqual(TrackerRules.site(for: "www.sony.co.uk"), "sony.co.uk")
        XCTAssertEqual(TrackerRules.site(for: "claude.ai"), "claude.ai")
    }

    func testBrowserURLParsing() {
        XCTAssertEqual(BrowserModel.url(from: "wsj.com")?.absoluteString, "https://wsj.com")
        XCTAssertEqual(BrowserModel.url(from: "https://a.b/c")?.absoluteString, "https://a.b/c")
        XCTAssertEqual(BrowserModel.url(from: "cookie law")?.host, "duckduckgo.com")
        XCTAssertNil(BrowserModel.url(from: "  "))
    }

    func testBlockerJSONIsValid() throws {
        let data = Data(ContentBlocker.rulesJSON().utf8)
        let rules = try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [[String: Any]])
        XCTAssertEqual(rules.count, TrackerRules.trackerHosts.count)
    }
}
