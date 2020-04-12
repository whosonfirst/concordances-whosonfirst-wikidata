#!/bin/bash
set -o errexit

#
#  https://tools.wmflabs.org/bambots/WikidataClasses.php?id=Q515&lang=en
#  https://tools.wmflabs.org/wikidata-todo/tree.html?q=2221906&rp=279
# 

source ./wdt_func.sh

# business
wdtclr business
wdtadd business Q4830453 "business"

# ------------------------------------
# localities
./wdt_localities.sh

#blacklist
./wdt_blacklist.sh

# disambiguation ----
wdtclr disambiguation
wdtadd disambiguation Q4167410  "Wikimedia disambiguation page"

# duplicated
wdtclr duplicated
wdtadd duplicated Q17362920  "Wikimedia duplicated page"


# wikimedia ----
wdtclr wikimedia
wdtadd wikimedia Q14204246 "Wikimedia project page "
wdtadd wikimedia Q17442446 "Wikimedia internal item"
wdtadd wikimedia Q13406463 "Wikimedia list article"

# uncategorized  ( and maybe important - so load to PostgreSQL )
wdtclr uncategorized
wdtadd uncategorized Q1077097  "tambon- central government unit in Thailand"
wdtadd uncategorized Q1080224  "electoral district of the Australian states and territories"
wdtadd uncategorized Q56061    "administrative territorial entity"
wdtadd uncategorized Q6633876  "list of places in Singapore"




# dependency
wdtclr dependency
wdtadd dependency Q161243 "dependent territory"
## --- added for Guantanamo Bay Naval Base
wdtcopy dependency Q1192403 "Concession"
##  -- copy for     Åland Islands
wdtcopy dependency Q2533461 "autonomous administrative territorial entity"
##  -- copy for Guam
wdtcopy dependency Q1048835 "political territorial entity "

# archipelago -------------------------------------------------
wdtclr archipelago
wdtadd archipelago Q33837 "archipelago"

# Basin -------------------------------------------------------
wdtclr basin
#wdtadd basin Q10438410 basin
wdtadd basin Q813672   basin
wdtadd basin Q749565   basin  "structural basin"
wdtadd basin Q166620   basin  "drainage basin"

# Bay ----
wdtclr bay
wdtadd bay Q39594 "bay"

# borough
wdtclr borough
wdtadd borough Q5195043 borough

# campus
wdtclr campus
wdtadd campus Q62447  aerodrome
wdtadd campus Q194188 spaceport

# canyon
wdtclr canyon
wdtadd canyon  Q150784 canyon

# cape
wdtclr cape
wdtadd cape Q185113 "cape"
wdtadd cape Q191992 "headland"

# circle
wdtclr circle
wdtadd circle Q146591 "circle of latitude"
wdtadd circle Q32099  "meridian"
wdtadd circle Q146657 "great circle"
wdtadd circle Q131389 "International Date Line"

# coast
wdtclr coast
wdtadd coast Q19817101 "coastal landform"

# continent
wdtclr continent
wdtadd continent Q5107  "Continent"

# country
wdtclr country
wdtadd country Q6256      "country"
wdtadd country Q3624078   "sovereign state"
wdtadd country Q15634554  "state with limited recognition"

# county
wdtclr county
wdtadd county  Q28575    "county"
wdtadd county  Q13360155 "county or county-equivalent"
wdtadd county  Q3301053  "consolidated city-county"
wdtadd county  Q713146   "county-controlled city"
wdtadd county  Q17143371 "county of South Korea"
wdtadd county  Q149621   "county of North Korea"
wdtadd county  Q149621   "districts"
wdtadd county  Q475061   "amphoe - Thailand "  ???

# dam
wdtclr dam
wdtadd dam Q12323 "dam"

# delta
wdtclr delta
wdtadd delta Q1233637 "river mouth"

#  depression
wdtclr depression
wdtadd depression Q22978151 "depression"

# desert
wdtclr desert
wdtadd desert Q8514 "desert"

# disputed
wdtclr disputed
wdtadd  disputed Q15239622 "disputed territory"
wdtadd  disputed Q2577883  "occupied territory"
wdtadd  disputed Q312461   "terra nullius"

# dmz
wdtclr dmz
wdtadd dmz Q41691 "demilitarized zone"

# fictional
wdtclr fictional
wdtadd fictional Q3895768 "fictional location"

# graben
wdtclr graben
wdtadd graben Q192810 "graben"

# island
wdtclr island
wdtadd island Q23442 "island"
wdtadd island Q42523 "atoll"

# isthmus
wdtclr isthmus
wdtadd isthmus Q93267 "isthmus"

# lake
wdtclr lake
wdtadd lake  Q23397    "lake"
wdtadd lake  Q1172903  "Loch"
wdtadd lake  Q2551525  "water reservoir"
wdtadd lake  Q3253281  "pond"

# lakegrp
wdtclr lakegrp
wdtadd lakegrp Q5926864 "group of lakes"

# landform
wdtclr landform
wdtadd landform Q271669 "landform"

# landscape
wdtclr landscape
wdtadd landscape Q107425 "landscape"

# localadmin
wdtclr localadmin
wdtadd localadmin Q188509 "Suburb"


# macrocounty
wdtclr macrocounty
wdtadd macrocounty Q194203 "arrondissement of France"
wdtadd macrocounty Q22721  "Regierungsbezirk"
wdtadd macrocounty Q706447 "county of Taiwan"


# macroregion
wdtclr macroregion
wdtadd macroregion Q254450 "planning and statistical region of Hungary "


# marinearea
wdtclr marinearea
wdtadd marinearea Q165     "sea"
wdtadd marinearea Q39594   "bay"
wdtadd marinearea Q37901   "strait"
wdtadd marinearea Q1322134 "gulf"
wdtadd marinearea Q4022    "river"
wdtadd marinearea Q23397   "lake"
wdtadd marinearea Q1210950 "channel"
wdtadd marinearea Q11292   "coral reef"
wdtadd marinearea Q45776   "fjord"
wdtadd marinearea Q1172599 "inlet"
wdtadd marinearea Q47053   "estuary"

# mountain
wdtclr mountain
wdtadd mountain Q8502    "Mountain"
wdtadd mountain Q1245089 "promontory"
wdtadd mountain Q8072    "volcano"
wdtadd mountain Q1437459 "non-geologically related mountain range"

# neighbourhood
wdtclr neighbourhood
wdtadd neighbourhood Q123705 "neighborhood"

# ocean
wdtclr ocean
wdtadd ocean Q9430 "Ocean"

# pass
wdtclr pass
wdtadd pass Q133056 "mountain pass"

# peninsula
wdtclr peninsula
wdtadd peninsula Q34763 "peninsula"

# plain
wdtclr plain
wdtadd plain Q160091  "plain"
wdtadd plain Q1006733 "grassland"
wdtadd plain Q123991  "steppe"
wdtadd plain Q194281  "prairie"
wdtadd plain Q184382  "pampas"
wdtadd plain Q31566   "coastal plain"

# planet
wdtclr planet
wdtadd planet Q3504248  "inner planet of the Solar System"

# plateau
wdtclr plateau
wdtadd plateau Q75520 "plateau"

# playa
wdtclr playa
wdtadd playa Q14253637 "dry lake"

# pole
wdtclr pole
wdtadd pole  Q183273 "geographical pole"

# port
wdtclr port
wdtadd port Q44782 "port"

# protectedarea
wdtclr protectedarea
wdtadd protectedarea Q473972 "protected area"

# region ?????
wdtclr region
cat manual_region.csv >> wikidata_region.csv

# research
wdtclr research
wdtadd research Q195339 "research station"

# river
wdtclr river
wdtadd river Q355304 "watercourse"

# timezone
wdtclr timezone
wdtadd timezone Q12143 "time zone"

# tundra
wdtclr tundra
wdtadd tundra Q43262 "tundra"

# valley
wdtclr valley
wdtadd valley Q39816 "valley"

# waterbody
wdtclr waterbody
wdtadd waterbody Q15324 "body of water"

# waterfall
wdtclr waterfall
wdtadd waterfall Q34038 "waterfall"

# wetland
wdtclr wetland
wdtadd wetland Q170321 "wetland"


