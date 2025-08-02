import geoip2.database

# Paths to your database files
asn_db_path = '/home/smilax/block-production/api/geolite2/GeoLite2-ASN.mmdb'
city_db_path = '/home/smilax/block-production/api/geolite2/GeoLite2-City.mmdb'
country_db_path = '/home/smilax/block-production/api/geolite2/GeoLite2-Country.mmdb'

def get_ip_info(ip_address):
    result = {}

    # ASN lookup
    with geoip2.database.Reader(asn_db_path) as reader:
        try:
            response = reader.asn(ip_address)
            result['asn'] = response.autonomous_system_number
            result['asn_org'] = response.autonomous_system_organization
        except geoip2.errors.AddressNotFoundError:
            result['asn'] = 'Not found'
            result['asn_org'] = 'Not found'

    # City lookup
    with geoip2.database.Reader(city_db_path) as reader:
        try:
            response = reader.city(ip_address)
            result['city'] = response.city.name
            result['latitude'] = response.location.latitude
            result['longitude'] = response.location.longitude
        except geoip2.errors.AddressNotFoundError:
            result['city'] = 'Not found'
            result['latitude'] = 'Not found'
            result['longitude'] = 'Not found'

    # Country lookup
    with geoip2.database.Reader(country_db_path) as reader:
        try:
            response = reader.country(ip_address)
            result['country'] = response.country.name
            result['country_iso'] = response.country.iso_code
        except geoip2.errors.AddressNotFoundError:
            result['country'] = 'Not found'
            result['country_iso'] = 'Not found'

    return result

# Example usage
ip = '8.8.8.8'  # Google's public DNS
info = get_ip_info(ip)
print(f"Information for IP {ip}:")
for key, value in info.items():
    print(f"{key}: {value}")