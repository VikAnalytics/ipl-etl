"""
etl/venue_resolver.py

Resolves any venue name variant to the canonical name stored in the DB.
Canonical = current official name, no city suffix (city lives in matches.city).

Usage:
    from etl.venue_resolver import resolve_venue

    resolve_venue("Feroz Shah Kotla")                     # → "Arun Jaitley Stadium"
    resolve_venue("Arun Jaitley Stadium, Delhi")          # → "Arun Jaitley Stadium"
    resolve_venue("M.Chinnaswamy Stadium")                # → "M Chinnaswamy Stadium"
    resolve_venue("Sardar Patel Stadium, Motera")         # → "Narendra Modi Stadium"
    resolve_venue("Eden Gardens, Kolkata")                # → "Eden Gardens"
    resolve_venue("Some New Ground")                      # → "Some New Ground" (passthrough)
"""

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)

# All known variants → canonical name.
# Keys are lowercased for lookup; values are the exact canonical string.
_ALIASES: dict[str, str] = {
    # ── Arun Jaitley Stadium (formerly Feroz Shah Kotla) ──
    "arun jaitley stadium":         "Arun Jaitley Stadium",
    "arun jaitley stadium, delhi":  "Arun Jaitley Stadium",
    "feroz shah kotla":             "Arun Jaitley Stadium",

    # ── Narendra Modi Stadium (formerly Sardar Patel / Motera) ──
    "narendra modi stadium":            "Narendra Modi Stadium",
    "narendra modi stadium, ahmedabad": "Narendra Modi Stadium",
    "sardar patel stadium, motera":     "Narendra Modi Stadium",

    # ── M Chinnaswamy Stadium ──
    "m chinnaswamy stadium":            "M Chinnaswamy Stadium",
    "m chinnaswamy stadium, bengaluru": "M Chinnaswamy Stadium",
    "m.chinnaswamy stadium":            "M Chinnaswamy Stadium",

    # ── MA Chidambaram Stadium ──
    "ma chidambaram stadium":                    "MA Chidambaram Stadium",
    "ma chidambaram stadium, chepauk":           "MA Chidambaram Stadium",
    "ma chidambaram stadium, chepauk, chennai":  "MA Chidambaram Stadium",

    # ── Eden Gardens ──
    "eden gardens":         "Eden Gardens",
    "eden gardens, kolkata": "Eden Gardens",

    # ── Wankhede Stadium ──
    "wankhede stadium":         "Wankhede Stadium",
    "wankhede stadium, mumbai": "Wankhede Stadium",

    # ── Brabourne Stadium ──
    "brabourne stadium":         "Brabourne Stadium",
    "brabourne stadium, mumbai": "Brabourne Stadium",

    # ── Dr DY Patil Sports Academy ──
    "dr dy patil sports academy":         "Dr DY Patil Sports Academy",
    "dr dy patil sports academy, mumbai": "Dr DY Patil Sports Academy",

    # ── Punjab Cricket Association IS Bindra Stadium ──
    "punjab cricket association is bindra stadium":                    "Punjab Cricket Association IS Bindra Stadium",
    "punjab cricket association is bindra stadium, mohali":            "Punjab Cricket Association IS Bindra Stadium",
    "punjab cricket association is bindra stadium, mohali, chandigarh": "Punjab Cricket Association IS Bindra Stadium",
    "punjab cricket association stadium, mohali":                      "Punjab Cricket Association IS Bindra Stadium",

    # ── Rajiv Gandhi International Stadium ──
    "rajiv gandhi international stadium":                  "Rajiv Gandhi International Stadium",
    "rajiv gandhi international stadium, uppal":           "Rajiv Gandhi International Stadium",
    "rajiv gandhi international stadium, uppal, hyderabad": "Rajiv Gandhi International Stadium",

    # ── Maharashtra Cricket Association Stadium (formerly Subrata Roy Sahara) ──
    "maharashtra cricket association stadium":       "Maharashtra Cricket Association Stadium",
    "maharashtra cricket association stadium, pune": "Maharashtra Cricket Association Stadium",
    "subrata roy sahara stadium":                    "Maharashtra Cricket Association Stadium",

    # ── Himachal Pradesh Cricket Association Stadium ──
    "himachal pradesh cricket association stadium":             "Himachal Pradesh Cricket Association Stadium",
    "himachal pradesh cricket association stadium, dharamsala": "Himachal Pradesh Cricket Association Stadium",

    # ── Sawai Mansingh Stadium ──
    "sawai mansingh stadium":        "Sawai Mansingh Stadium",
    "sawai mansingh stadium, jaipur": "Sawai Mansingh Stadium",

    # ── Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium ──
    "dr. y.s. rajasekhara reddy aca-vdca cricket stadium":                  "Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium",
    "dr. y.s. rajasekhara reddy aca-vdca cricket stadium, visakhapatnam":   "Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium",

    # ── Barsapara Cricket Stadium ──
    "barsapara cricket stadium":          "Barsapara Cricket Stadium",
    "barsapara cricket stadium, guwahati": "Barsapara Cricket Stadium",

    # ── Maharaja Yadavindra Singh International Cricket Stadium ──
    # Mullanpur and New Chandigarh refer to the same location
    "maharaja yadavindra singh international cricket stadium, mullanpur":     "Maharaja Yadavindra Singh International Cricket Stadium",
    "maharaja yadavindra singh international cricket stadium, new chandigarh": "Maharaja Yadavindra Singh International Cricket Stadium",

    # ── Vidarbha Cricket Association Stadium ──
    "vidarbha cricket association stadium, jamtha": "Vidarbha Cricket Association Stadium",

    # ── Zayed Cricket Stadium (formerly Sheikh Zayed Stadium) ──
    "zayed cricket stadium":            "Zayed Cricket Stadium",
    "zayed cricket stadium, abu dhabi": "Zayed Cricket Stadium",
    "sheikh zayed stadium":             "Zayed Cricket Stadium",

    # ── Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium ──
    "bharat ratna shri atal bihari vajpayee ekana cricket stadium, lucknow": "Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium",

    # ── Single-name venues (no suffix variants seen, but register for safety) ──
    "barabati stadium":                         "Barabati Stadium",
    "green park":                               "Green Park",
    "holkar cricket stadium":                   "Holkar Cricket Stadium",
    "jsca international stadium complex":       "JSCA International Stadium Complex",
    "nehru stadium":                            "Nehru Stadium",
    "saurashtra cricket association stadium":   "Saurashtra Cricket Association Stadium",
    "shaheed veer narayan singh international stadium": "Shaheed Veer Narayan Singh International Stadium",
    "sharjah cricket stadium":                  "Sharjah Cricket Stadium",
    "dubai international cricket stadium":      "Dubai International Cricket Stadium",

    # ── South African venues (used during 2009 IPL) ──
    "buffalo park":          "Buffalo Park",
    "de beers diamond oval": "De Beers Diamond Oval",
    "kingsmead":             "Kingsmead",
    "new wanderers stadium": "New Wanderers Stadium",
    "newlands":              "Newlands",
    "outsurance oval":       "OUTsurance Oval",
    "st george's park":      "St George's Park",
    "supersport park":       "SuperSport Park",
}


# City name normalization
# Keys lowercased; values are canonical city names.
_CITY_ALIASES: dict[str, str] = {
    # Old spelling → official renamed spelling
    "bangalore": "Bengaluru",
    # Cricsheet used "Mohali" for Maharaja Yadavindra Singh stadium in 2024,
    # then switched to "New Chandigarh" — consolidate to the latter (correct location)
    "mohali":    "New Chandigarh",
}


def resolve_city(name: str | None) -> str | None:
    """Return canonical city name. Passes through unknowns unchanged."""
    if not name:
        return name
    return _CITY_ALIASES.get(name.lower().strip(), name)


def resolve_venue(name: str | None) -> str | None:
    """
    Return the canonical venue name for any input variant.
    Passes through unknown names unchanged (with a warning).
    """
    if not name:
        return name
    resolved = _ALIASES.get(name.lower().strip())
    if resolved is None:
        logger.warning(f"Unknown venue: '{name}' — passing through unchanged")
        return name
    return resolved
