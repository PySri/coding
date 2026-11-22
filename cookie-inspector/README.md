# cookie-inspector

Scan a folder of browser cookie stores, describe every cookie, and optionally delete cookies by category.

Single Python file, standard library only (Python 3.8+).

## Supported formats
Chromium browsers (Chrome, Edge, Brave, Opera, Vivaldi), Firefox, Safari (read-only),
Netscape `cookies.txt`, JSON exports (EditThisCookie, Cookie-Editor, Playwright), legacy IE/WinINet files.

## Report
```
python cookie_inspector.py /path/to/cookies -o report.html --csv cookies.csv --json cookies.json
python cookie_inspector.py --locations     # where browsers keep cookies on this OS
```
For each cookie: vendor, purpose category, description, domain/path, created/expires/last-accessed,
lifetime, Secure/HttpOnly/SameSite flags, value shape, and security/privacy findings.
Values are hidden unless `--show-values`; encrypted Chromium values are never decrypted.

## Deleting by category
```
python cookie_inspector.py ./cookies --delete Advertising,Analytics            # dry run
python cookie_inspector.py ./cookies --delete Advertising,Analytics --apply    # delete
python cookie_inspector.py ./cookies --delete-expired --apply
python cookie_inspector.py ./cookies --delete ads --keep-domain google.com --apply
```

## Deleting specific cookies
```
python cookie_inspector.py ./cookies --delete-id 3f9a1c2e --apply             # one exact cookie (ID from report)
python cookie_inspector.py ./cookies --delete Functional --delete-name lang   # a named cookie within a category
python cookie_inspector.py ./cookies --delete-domain example.com --apply      # every cookie for a domain
python cookie_inspector.py ./cookies --delete Analytics --delete-domain example.com --apply
python cookie_inspector.py ./cookies --delete-name "_ga*" --apply             # wildcards allowed
```
Selectors combine with AND (each narrows the selection); several values for the same option
combine with OR. `--delete-expired` means "only expired" (on its own: all expired cookies).
Every cookie in the HTML report shows its ID and has buttons that copy the matching delete command.
Categories: `Strictly necessary`, `Authentication`, `Security`, `Functional`, `Consent`,
`Analytics`, `Advertising`, `Unknown`, `all` (aliases: ads, marketing, tracking, stats, auth, essential…).

- Dry run unless `--apply`; asks for confirmation unless `--yes`.
- Backs up each changed file as `<file>.bak-<timestamp>` (disable with `--no-backup`).
- Refuses to edit a file while its browser is running or the file is locked (`--force` overrides the process check).
- **Close the browser completely before deleting.**
