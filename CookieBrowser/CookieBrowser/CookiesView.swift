import SwiftUI

extension CookieCategory {
    var color: Color {
        switch self {
        case .advertising: .red
        case .analytics: .orange
        case .signIn: .green
        case .other: .gray
        }
    }
}

struct CategoryBadge: View {
    let category: CookieCategory
    var body: some View {
        Text(category.rawValue)
            .font(.caption2.weight(.medium))
            .padding(.horizontal, 6).padding(.vertical, 2)
            .background(category.color.opacity(0.15), in: Capsule())
            .foregroundStyle(category.color)
    }
}

struct CookiesView: View {
    @ObservedObject var manager: CookieManager
    @AppStorage("autoClean") private var autoClean = true
    @AppStorage("blockTrackers") private var blockTrackers = true
    @State private var filter: CookieCategory?
    @State private var confirmAll = false

    private var groups: [SiteGroup] {
        guard let filter else { return manager.sites }
        return manager.sites.compactMap { g in
            let c = g.cookies.filter { $0.category == filter }
            return c.isEmpty ? nil : SiteGroup(site: g.site, cookies: c)
        }
    }

    var body: some View {
        NavigationStack {
            List {
                Section {
                    HStack {
                        ForEach(CookieCategory.allCases) { cat in
                            Button {
                                filter = filter == cat ? nil : cat
                            } label: {
                                VStack(spacing: 2) {
                                    Text("\(manager.count(cat))").font(.title3.weight(.semibold))
                                    Text(cat.rawValue).font(.caption2)
                                }
                                .frame(maxWidth: .infinity)
                                .padding(.vertical, 6)
                                .background(filter == cat ? cat.color.opacity(0.2) : .clear,
                                            in: RoundedRectangle(cornerRadius: 8))
                                .foregroundStyle(cat.color)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    Button(role: .destructive) {
                        Task { await manager.deleteTrackers() }
                    } label: {
                        Label("Delete \(manager.trackerCount) tracker cookies", systemImage: "trash")
                    }
                    .disabled(manager.trackerCount == 0)
                    .accessibilityIdentifier("deleteTrackers")
                    if let last = manager.lastCleanup {
                        Text(last).font(.footnote).foregroundStyle(.secondary)
                    }
                } footer: {
                    Text("\(manager.cookies.count) cookies from \(manager.sites.count) sites. Tap a category to filter.")
                }

                Section("Settings") {
                    Toggle("Delete trackers after each page", isOn: $autoClean)
                    Toggle("Block tracker sites", isOn: $blockTrackers)
                    Button("Delete all website data", role: .destructive) { confirmAll = true }
                }

                Section(filter.map { "\($0.rawValue) cookies by site" } ?? "Cookies by site") {
                    if groups.isEmpty {
                        Text("No cookies yet. Browse a few sites in the Browser tab.")
                            .foregroundStyle(.secondary)
                    }
                    ForEach(groups) { group in
                        NavigationLink {
                            SiteDetailView(manager: manager, site: group.site)
                        } label: {
                            HStack {
                                Text(group.site)
                                Spacer()
                                if group.trackerCount > 0 {
                                    Text("\(group.trackerCount) tracker\(group.trackerCount == 1 ? "" : "s")")
                                        .font(.caption).foregroundStyle(.red)
                                }
                                Text("\(group.cookies.count)").foregroundStyle(.secondary)
                            }
                        }
                        .swipeActions {
                            Button("Delete", role: .destructive) {
                                Task { await manager.deleteSite(group.site) }
                            }
                        }
                    }
                }
            }
            .navigationTitle("Cookies")
            .refreshable { await manager.refresh() }
            .task { await manager.refresh() }
            .confirmationDialog("Delete all website data?", isPresented: $confirmAll, titleVisibility: .visible) {
                Button("Delete everything", role: .destructive) {
                    Task { await manager.deleteAllWebsiteData() }
                }
            } message: {
                Text("Removes every cookie and stored site data in this browser. You'll be signed out of all sites.")
            }
        }
    }
}

struct SiteDetailView: View {
    @ObservedObject var manager: CookieManager
    let site: String

    private var items: [CookieItem] { manager.cookies.filter { $0.site == site }.sorted { $0.cookie.name < $1.cookie.name } }

    var body: some View {
        List {
            ForEach(items) { item in
                VStack(alignment: .leading, spacing: 4) {
                    HStack {
                        Text(item.cookie.name).font(.body.monospaced()).lineLimit(1)
                        Spacer()
                        CategoryBadge(category: item.category)
                    }
                    Text(details(item)).font(.caption).foregroundStyle(.secondary)
                }
                .swipeActions {
                    Button("Delete", role: .destructive) { Task { await manager.delete([item]) } }
                }
            }
        }
        .navigationTitle(site)
        .toolbar {
            Button("Delete all", role: .destructive) { Task { await manager.deleteSite(site) } }
        }
    }

    private func details(_ item: CookieItem) -> String {
        let c = item.cookie
        var parts = [item.host]
        if let exp = c.expiresDate {
            parts.append("expires " + exp.formatted(date: .abbreviated, time: .omitted))
        } else {
            parts.append("session")
        }
        if c.isSecure { parts.append("Secure") }
        if c.isHTTPOnly { parts.append("HttpOnly") }
        return parts.joined(separator: " · ")
    }
}
