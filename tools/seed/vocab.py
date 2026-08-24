"""Compositional vocabulary for synthetic domain text.

Why this exists rather than a generic fake-text library:

The L0 demo turns on semantic search actually working. If product descriptions are
lorem ipsum or unstructured fake sentences, embeddings of them carry no real
structure -- "waterproof hiking backpack" retrieves nothing meaningful, the ANN
channel becomes noise, and the whole freshness argument collapses because there is
nothing sensible for the stale channel to be confidently wrong *about*.

So descriptions are composed from a controlled vocabulary organised by category.
Items in the same category share materials, features and use cases; items across
categories share almost none. That produces genuine semantic clusters, which means
cosine similarity behaves the way it would on a real catalogue -- and it does so
without shipping a licensed dataset.

Deliberately no external dependency: `random.Random(seed)` makes the whole corpus
reproducible, which experiments E3 and E4 need.
"""
from __future__ import annotations

# Each category is a self-contained semantic neighbourhood.
CATEGORIES: dict[str, dict[str, list[str]]] = {
    "Outdoor & Hiking": {
        "types": ["backpack", "tent", "sleeping bag", "trekking pole", "headlamp",
                  "water bottle", "rain jacket", "hiking boot", "camp stove", "dry bag"],
        "materials": ["ripstop nylon", "waxed canvas", "recycled polyester",
                      "anodised aluminium", "vulcanised rubber"],
        "features": ["waterproof", "ultralight", "packable", "reinforced seams",
                     "ventilated", "abrasion-resistant", "quick-drying"],
        "uses": ["multi-day treks", "alpine ascents", "wet-weather hiking",
                 "backcountry camping", "trail running"],
    },
    "Kitchen & Dining": {
        "types": ["chef knife", "cast-iron skillet", "stock pot", "cutting board",
                  "espresso maker", "food storage set", "mixing bowl", "kettle"],
        "materials": ["forged carbon steel", "enamelled cast iron", "borosilicate glass",
                      "end-grain walnut", "brushed stainless steel"],
        "features": ["dishwasher safe", "induction compatible", "non-stick",
                     "heat-retaining", "ergonomic handle", "stain-resistant"],
        "uses": ["everyday cooking", "slow braising", "high-heat searing",
                 "meal preparation", "home baking"],
    },
    "Audio & Headphones": {
        "types": ["over-ear headphone", "wireless earbud", "bookshelf speaker",
                  "portable speaker", "USB microphone", "turntable", "audio interface"],
        "materials": ["memory foam", "machined aluminium", "acoustic-grade MDF",
                      "protein leather", "neodymium driver"],
        "features": ["active noise cancelling", "40-hour battery", "low-latency",
                     "hi-res certified", "foldable", "sweat-resistant"],
        "uses": ["daily commuting", "studio monitoring", "focused work",
                 "home listening", "video calls"],
    },
    "Fitness & Training": {
        "types": ["yoga mat", "kettlebell", "resistance band set", "foam roller",
                  "jump rope", "adjustable dumbbell", "exercise bike"],
        "materials": ["closed-cell TPE", "cast iron with vinyl coating",
                      "natural latex", "high-density EVA", "powder-coated steel"],
        "features": ["non-slip", "space-saving", "adjustable resistance",
                     "extra-thick", "odour-free", "floor-protecting"],
        "uses": ["home workouts", "strength training", "post-exercise recovery",
                 "mobility work", "high-intensity intervals"],
    },
    "Office & Desk": {
        "types": ["standing desk", "ergonomic chair", "monitor arm", "desk lamp",
                  "mechanical keyboard", "laptop stand", "cable organiser"],
        "materials": ["solid bamboo", "mesh-backed polymer", "cold-rolled steel",
                      "anodised aluminium", "PBT keycap"],
        "features": ["height-adjustable", "lumbar support", "tool-free assembly",
                     "flicker-free", "hot-swappable", "cable-managed"],
        "uses": ["long working sessions", "small home offices", "dual-monitor setups",
                 "shared workspaces", "focused study"],
    },
    "Pet Supplies": {
        "types": ["dog harness", "cat tree", "pet carrier", "feeding bowl",
                  "grooming brush", "chew toy", "waterproof bed"],
        "materials": ["padded neoprene", "sisal rope", "chew-resistant nylon",
                      "food-grade silicone", "sherpa fleece"],
        "features": ["escape-proof", "machine washable", "reflective trim",
                     "non-tip", "odour-controlling", "veterinarian recommended"],
        "uses": ["daily walks", "long car journeys", "anxious pets",
                 "large breeds", "indoor cats"],
    },
    "Garden & Outdoor Living": {
        "types": ["pruning shear", "watering can", "raised planter", "garden hose",
                  "compost bin", "patio umbrella", "solar path light"],
        "materials": ["powder-coated steel", "FSC-certified cedar", "UV-stable resin",
                      "galvanised metal", "hardened carbon steel"],
        "features": ["rust-resistant", "self-watering", "frost-proof",
                     "tool-free assembly", "weatherproof", "ergonomic grip"],
        "uses": ["small balconies", "vegetable beds", "year-round outdoor use",
                 "container gardening", "seasonal pruning"],
    },
    "Baby & Toddler": {
        "types": ["stroller", "high chair", "baby carrier", "night light",
                  "bottle steriliser", "playmat", "travel cot"],
        "materials": ["OEKO-TEX cotton", "BPA-free polypropylene", "beech hardwood",
                      "breathable mesh", "food-grade silicone"],
        "features": ["one-hand fold", "machine washable", "adjustable recline",
                     "hypoallergenic", "compact-folding", "toppling-resistant"],
        "uses": ["newborns", "city travel", "mealtimes", "nap routines",
                 "growing toddlers"],
    },
}

# Sentence frames. Several shapes, so the corpus does not read as one repeated
# template -- which would make every embedding in a category near-identical and
# destroy the ranking signal the demo depends on.
# Materials are category-scoped but not type-scoped, so a frame must never let a
# material directly premodify a type -- that yields "borosilicate glass cast-iron
# skillet". Every frame below keeps them in separate phrases.
DESCRIPTION_FRAMES = [
    "{article} {adj} {type} built for {use}, made from {material}. {feature_sentence}",
    "{feature_sentence} Made from {material}, this {type} is designed for {use}.",
    "Made from {material}, this {adj} {type} suits {use}. {feature_sentence}",
    "For {use}: {article_lc} {adj} {type}, finished in {material}. {feature_sentence}",
]

ADJECTIVES = ["compact", "durable", "lightweight", "professional-grade", "everyday",
              "premium", "minimalist", "heavy-duty", "versatile", "purpose-built"]

BRANDS = ["Northvale", "Kestrel", "Ironwood", "Lumen", "Baseline", "Harbour",
          "Tallgrass", "Meridian", "Copperfield", "Alder & Co", "Vantage", "Rookline"]

REGIONS = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Can Tho", "Hai Phong",
           "Singapore", "Bangkok", "Jakarta", "Kuala Lumpur", "Manila"]

WAREHOUSES = ["HAN-01", "HAN-02", "SGN-01", "SGN-02", "DAD-01", "SIN-01"]

FAMILY_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui",
                "Do", "Ho", "Ngo", "Duong", "Ly", "Phan", "Truong"]

GIVEN_NAMES = ["An", "Binh", "Chi", "Dung", "Giang", "Ha", "Hieu", "Khanh", "Lan",
               "Linh", "Mai", "Minh", "Nam", "Ngoc", "Phuc", "Quang", "Son", "Thao",
               "Trang", "Tuan", "Vy", "Yen"]


def compose_product(rng, category: str, ptype: str) -> tuple[str, str]:
    """Name and description for one product, as a matched pair.

    These MUST be generated together. Drawing the type independently for each
    produces items like "Minimalist Kettle" described as a stock pot: the embedding
    is computed from the description while the console displays the name, so a
    perfectly correct retrieval reads as a broken one. The caller passes `ptype`
    because the leaf category has to agree with both.
    """
    c = CATEGORIES[category]
    adj = rng.choice(ADJECTIVES)
    feats = rng.sample(c["features"], k=min(2, len(c["features"])))
    description = rng.choice(DESCRIPTION_FRAMES).format(
        article="An" if adj[0] in "aeiou" else "A",
        article_lc="an" if adj[0] in "aeiou" else "a",
        adj=adj,
        material=rng.choice(c["materials"]),
        type=ptype,
        use=rng.choice(c["uses"]),
        feature_sentence=f"{feats[0].capitalize()} and {feats[1]}.",
    )
    name = f"{rng.choice(BRANDS)} {adj.capitalize()} {ptype.title()}"
    return name, description


def compose_person(rng) -> str:
    return f"{rng.choice(FAMILY_NAMES)} {rng.choice(GIVEN_NAMES)} {rng.choice(GIVEN_NAMES)}"
