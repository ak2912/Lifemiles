import requests, re
import csv

URL = "http://www.staralliance.com/destination_overview1.do?language=en&method=fetchAirports&country={0}"


COUNTRY_CODES = ['AF','AL','DZ','AO','AG','AR','AM','AW','AU','AT','AZ','BS','BH','BD','BB','BY','BE','BZ','BJ','BM','BO','BQ','BA','BW','BR','BN','BG','BF','BI','KH','CM','CA','CV','KY','TD','CL','CN','CO','CG','CK','CR','CI','HR','CU','CW','CY','CZ','CD','DK','DJ','DO','EC','EG','SV','GQ','ER','EE','ET','FJ','FI','FR','PF','GA','GM','GE','DE','GH','GR','GD','GP','GU','GT','GN','GY','HT','HN','HK','HU','IS','IN','ID','IR','IQ','IE','IL','IT','JM','JP','JE','JO','KZ','KE','KR','KP','KW','KG','LA','LV','LB','LR','LY','LT','LU','MO','MK','MW','MY','MV','ML','MT','MH','MQ','MR','MU','MX','FM','MD','MN','ME','MA','MZ','MM','NA','NP','NL','NC','NZ','NI','NE','NG','NU','NF','MP','NO','OM','PK','PW','PA','PY','PE','PH','PL','PT','PR','QA','RO','RU','RW','KN','LC','WS','ST','SA','SN','RS','SC','SL','SG','SX','SK','SI','SO','ZA','SS','ES','LK','SD','SE','CH','TW','TJ','TZ','TH','TG','TO','TT','TN','TR','TM','TC','UG','UA','AE','GB','US','UY','UZ','VU','VE','VN','VI','YE','ZM','ZW']

# these are the only origin/destination airports yielding airmiles that were
# found among initially scraped routes.
AIRPORTS_SUBSET = {"AAL", "AAR", "ABE", "ABJ", "ABV", "ABZ", "ACC", "ADA", "ADB", "ADD", "ADF", 
 "AER", "AES", "AGH", "ALA", "ALF", "ALG", "AMH", "AMM", "AMS", "ARN", "ASB", 
 "ASM", "ASR", "ASW", "ATH", "ATL", "ATW", "AUH", "AVP", "AXM", "AXT", "AXU", 
 "AYT", "BAH", "BAL", "BAQ", "BCN", "BEG", "BEY", "BGM", "BGO", "BGW", "BHM", 
 "BHX", "BHY", "BIO", "BJL", "BJM", "BJR", "BJV", "BJX", "BKI", "BKK", "BKO", 
 "BLL", "BLQ", "BOD", "BOG", "BOO", "BOS", "BRE", "BRI", "BRU", "BSL", "BSR", 
 "BUD", "BUR", "BUS", "BWI", "BZE", "CAI", "CAN", "CCU", "CDG", "CEB", "CFU", 
 "CGK", "CGN", "CGO", "CGQ", "CHQ", "CID", "CJU", "CKG", "CLE", "CLT", "CMH", 
 "CMN", "CNS", "CNX", "COO", "CPH", "CRK", "CSX", "CTS", "CTU", "CUC", "CVG", 
 "CZX", "DAC", "DAR", "DAX", "DAY", "DBV", "DCA", "DDG", "DEL", "DIY", "DKR", 
 "DLA", "DLC", "DLH", "DLM", "DME", "DMM", "DNH", "DNK", "DNZ", "DOH", "DPS", 
 "DRS", "DSM", "DTW", "DUB", "DUS", "DXB", "DYU", "EBB", "EBL", "ECN", "EDI", 
 "EDO", "ELM", "ELQ", "ERC", "ERI", "ERZ", "ESB", "EUG", "EVN", "EWR", "EZE", 
 "EZS", "FAO", "FBM", "FCO", "FLL", "FNC", "FNT", "FOC", "FOR", "FRA", "FRU", 
 "FSD", "FUK", "FWA", "GDL", "GDN", "GIG", "GMP", "GNY", "GOJ", "GOT", "GRB", 
 "GRR", "GRU", "GRZ", "GSO", "GSP", "GUM", "GVA", "GYD", "GYS", "GZP", "GZT", 
 "HAC", "HAJ", "HAK", "HAM", "HAN", "HAU", "HBE", "HEL", "HER", "HFE", "HGH", 
 "HIJ", "HJR", "HKD", "HKG", "HKT", "HMB", "HND", "HNL", "HRB", "HRG", "HSG", 
 "HTY", "IAD", "IAH", "IAS", "ICN", "ICT", "IEV", "IKA", "INC", "IND", "INN", 
 "ISB", "ISE", "ISG", "IST", "ISU", "ITH", "ITM", "IWJ", "JAN", "JAX", "JED", 
 "JFK", "JGS", "JIB", "JMU", "JNB", "JTR", "JUB", "JZH", "KAN", "KBL", "KBP", 
 "KBV", "KCM", "KCZ", "KHH", "KHI", "KHN", "KIV", "KIX", "KKC", "KLR", "KLU", 
 "KMG", "KMI", "KMJ", "KOJ", "KOW", "KRK", "KRR", "KRS", "KRT", "KSC", "KSH", 
 "KSU", "KSY", "KTW", "KUH", "KUL", "KWE", "KWI", "KWJ", "KWL", "KYA", "KZN", 
 "KZR", "LAD", "LAN", "LAR", "LAS", "LAX", "LBV", "LCA", "LED", "LEJ", "LET", 
 "LEX", "LFT", "LGW", "LHR", "LHW", "LIN", "LIS", "LIT", "LJU", "LLA", "LLW", 
 "LNK", "LNZ", "LOS", "LPB", "LRD", "LUN", "LUX", "LWO", "LXA", "LXR", "LYS", 
 "LZH", "MAD", "MAF", "MAN", "MCI", "MCO", "MCT", "MDE", "MED", "MEM", "MEX", 
 "MFE", "MFM", "MIA", "MIG", "MLA", "MLI", "MLX", "MMX", "MNL", "MOB", "MOL", 
 "MPM", "MQM", "MRS", "MSN", "MSP", "MSQ", "MSR", "MSY", "MUC", "MXP", "MYJ", 
 "MZR", "NAP", "NAT", "NAV", "NBO", "NCE", "NDG", "NDJ", "NGB", "NGO", "NGS", 
 "NIM", "NJF", "NKG", "NLA", "NNG", "NRT", "NSI", "NTE", "NTG", "NTQ", "NUE", 
 "ODS", "OIM", "OIT", "OKA", "OKC", "OKJ", "OMA", "ONJ", "OPO", "ORD", "OSD", 
 "OSL", "OTP", "OVB", "PAH", "PBI", "PEI", "PEK", "PEN", "PHC", "PHL", "PHX", 
 "PIT", "PMI", "PNH", "PNS", "POZ", "PRG", "PRN", "PUS", "PVG", "QRO", "RAI", 
 "RAP", "RDU", "REC", "RGN", "RHO", "RIC", "RIX", "RNB", "ROA", "ROV", "RSU", 
 "RSW", "RTM", "RUH", "RZE", "SAL", "SAT", "SBA", "SBN", "SBZ", "SDL", "SFO", 
 "SFT", "SGN", "SHA", "SHE", "SHV", "SIN", "SJC", "SJJ", "SKG", "SKP", "SMF", 
 "SMR", "SMX", "SOF", "SRQ", "SSA", "SSG", "SSH", "STL", "STR", "SUB", "SVG", 
 "SVO", "SWA", "SYO", "SYR", "SYX", "SYZ", "SZF", "SZG", "SZX", "SZZ", "TAK", 
 "TAM", "TAO", "TAS", "TBS", "TGD", "TGU", "TIA", "TKS", "TLL", "TLM", "TLS", 
 "TLV", "TMS", "TOS", "TOY", "TPA", "TPE", "TRN", "TSE", "TSN", "TTJ", "TUL", 
 "TUN", "TXL", "TZX", "UBJ", "UFA", "UKB", "UME", "URC", "USM", "USN", "UUS", 
 "VAS", "VCE", "VIE", "VKO", "VLC", "VNO", "VTE", "WAW", "WNZ", "WRO", "WUA", 
 "WUH", "WUX", "XFN", "XIY", "XMN", "YAM", "YBP", "YCU", "YEI", "YGJ", "YHZ", 
 "YIH", "YIN", "YIW", "YNT", "YNZ", "YOW", "YQB", "YQG", "YUL", "YXU", "YYJ", 
 "YYT", "YYY", "YYZ", "ZAG", "ZNZ", "ZRH", "ZUH"}


responses = []
airport_codes = []

def get_airport_codes(text, country):
	matches = re.findall(r'\[".*?/([a-z]{3})/", "(.*?), (.*?) ?, ([A-Z]{3})"]', text)
	for (code, city, name, CODE) in matches:
		if code.upper() != CODE:
			print("Mismatch: {0}/{1}".format(code, CODE))
		if ',' in name:
			name = name.replace(',', '-')
		yield [CODE, name, city, country]


for country in COUNTRY_CODES:
	url = URL.format(country)
	r = requests.get(url)
	codes = get_airport_codes(r.text, country)
	for code in codes:
		airport_codes += [code]


with open('airports.csv', 'w', newline='') as f:
	a = csv.writer(f, delimiter=',')
	data = airport_codes
	a.writerows(data)

airports_subset = [a for a in airport_codes if a[0] in AIRPORTS_SUBSET]
with open('airports_subset.csv', 'w', newline='') as f:
	a = csv.writer(f, delimiter=',')
	data = airports_subset
	a.writerows(data)
