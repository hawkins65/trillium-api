import geoip2.database

def print_fields(db_path, db_name):
    reader = geoip2.database.Reader(db_path)
    
    test_ip = '66.165.236.158'
    
    try:
        if db_name == 'ASN':
            record = reader.asn(test_ip)
        elif db_name == 'Country':
            record = reader.country(test_ip)
        else:  # City database
            record = reader.city(test_ip)
        
        print(f"\nFields available in {db_name} database:")
        
        def print_fields_recursive(obj, prefix=''):
            if isinstance(obj, (geoip2.models.ASN, geoip2.models.City, geoip2.models.Country, geoip2.records.Place)):
                for attr in dir(obj):
                    if not attr.startswith('_') and attr != 'raw':
                        value = getattr(obj, attr)
                        if callable(value):
                            continue
                        print_fields_recursive(value, prefix + attr + '.')
            elif isinstance(obj, dict):
                for key, value in obj.items():
                    print(f"{prefix}{key}")
            else:
                print(prefix.rstrip('.'))
        
        print_fields_recursive(record)
    
    except geoip2.errors.AddressNotFoundError:
        print(f"IP not found in {db_name} database")
    finally:
        reader.close()

# Paths to the MMDB files
print_fields('/home/smilax/block-production/api/geolite2/GeoLite2-ASN.mmdb', 'ASN')
print_fields('/home/smilax/block-production/api/geolite2/GeoLite2-City.mmdb', 'City')
print_fields('/home/smilax/block-production/api/geolite2/GeoLite2-Country.mmdb', 'Country')