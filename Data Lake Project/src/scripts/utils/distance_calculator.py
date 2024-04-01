import pyspark.sql.functions as F

def calculate_distance(msg_lat_rad, msg_lng_rad, city_lat_rad, city_lng_rad):
    """Calculates the Haversine distance between two points on Earth.

    Args:
        msg_lat_rad (float): Latitude of the message location in radians.
        msg_lng_rad (float): Longitude of the message location in radians.
        city_lat_rad (float): Latitude of the city location in radians.
        city_lng_rad (float): Longitude of the city location in radians.

    Returns:
        float: Distance between the two points in kilometers.
    """

    R = 6371  # Earth's radius in kilometers 

    delta_lat = F.abs(msg_lat_rad - city_lat_rad)
    delta_lng = F.abs(msg_lng_rad - city_lng_rad)

    a = F.sin(delta_lat / 2) ** 2 + F.cos(msg_lat_rad) * F.cos(city_lat_rad) * F.sin(delta_lng / 2) ** 2

    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

    distance = R * c
    
    return distance
