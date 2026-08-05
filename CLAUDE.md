# mesonet-photos

The Montana Mesonet Photo Explorer: a static MapLibre single-page app in `docs/`
(GitHub Pages root), plus the Python photo-mirroring pipeline in `scripts/`.

## House style

This app consumes mco-web-style (pinned + SRI in `docs/index.html`; currently
**v0.6.0** — check the tag in that file rather than trusting this line). Design tokens, a11y mandates, and interaction conventions: see
HOUSE-STYLE.md in https://github.com/mt-climate-office/mco-web-style — tokens
only (no raw hexes), `--accent` is fill-only, `aria-pressed` drives toggle
styling, canvas data needs a live region + sr-only table twin. To change shared
styling, change the kit and bump the pinned version here; never patch a local
copy.

App-local by deliberate kit decision (do NOT extract): the photo-mosaic
machinery, the gallery/lightbox dialogs, the date stepper, the direction
segments + `<select>` fallback, `updateSocialMeta`, and the branded PNG export.

Marked kit-overrides in this app:
- **No hillshade** — the photo mosaic is the figure; relief under opaque photo
  rasters would only show in the untiled west while competing with the imagery.
- Navbar wraps at ≤1060px and lifts `.nav-meta` beside the brand there, so the
  controls get a full-width row. The brand itself collapses to the logo badge at
  ≤750px — that is the kit default, not an override. Search collapsing to an icon
  + overlay at ≤640px is the kit's `.mco-search-collapse` component (this app
  prototyped it; mesonet-status adopting it is what moved it into the kit).

## Landing-slot fallback

`computeMaxTimestep` assumes a flat 30-minute processing lag, but the mirror job
publishes ~12×/day — so the newest expected slot is empty right after it turns
over and then fills in gradually (observed climbing 0% → 50% → 75% over minutes;
a settled slot measures ~83%, the rest being cameras that are simply offline).

`resolveInitialTimestep()` therefore probes a spread sample of stations before
the layers are added and picks the newest slot worth showing: it accepts a slot
at ≥60% of the sample, otherwise walks back up to 4 slots and takes the most
complete one. **It only runs when neither `?date` nor `?time` is present** — an
explicit or shared URL is never silently moved. If a run lands on an unexpected
date, this is why; `?date=`/`?time=` pins it.

The probed crops go into `_cropCache`, so the render reuses them. `?export=`
benefits too, which is the point: the social card used to be regenerable as a
blank map.

## Deploying — read before you push

Pushing `main` **is a production deploy, on two URLs**: GitHub Pages publishes
`/docs` from `main`, and the same page is reverse-proxied at
`mesonet.climate.umt.edu/photos/` (mesonet_app Caddyfile).

`.github/workflows/mirror_photos.yml` runs ~12×/day and **commits
`docs/preview.png` back to `main`** — always pull/rebase before pushing, or you
race it. `adjust_dst.yml` rewrites the crons on DST Sundays.

`scripts/generate_preview.py` drives the live page headlessly via `?export=dark`
plus a 4-second delay before clicking `#btn-export`. That timing, the param, and
the button id are a contract — changing them silently breaks the social preview
in production. Run it locally against your changes first.

## Verification

There is no CI for the page. Before any push, run the manual gates from
mco-web-style `MIGRATING.md` § "Verification recipe": `node --check docs/app.js`,
`npx html-validate@9 docs/index.html`, and the app's `consumer-verify.mjs`
harness (untracked; install `playwright` + `@axe-core/playwright` with
`--no-save`). Note two gotchas that cost time here:
- The harness's `renderEvidence` must be a **function**, not a string — a string
  predicate is `eval`'d in-page and the CSP has no `'unsafe-eval'`.
- `connect-src` must include `data:`: MapLibre fetches an `image` source's url,
  and every photo is a cover-cropped canvas data URL. Without it the entire
  mosaic silently fails to paint.
- Stations with no photo at the selected timestep return **403** (not 404) from
  CloudFront. Those console errors are expected and identical on production.
