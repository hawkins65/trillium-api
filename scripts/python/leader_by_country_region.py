import psycopg2
from collections import defaultdict
import pandas as pd
import json
import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from PIL import Image
import urllib.request
import numpy as np

def calculate_stake_statistics(json_file, epoch):
    # Load JSON data from file
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Define country to region mapping
    country_to_region = {
        "Australia": "ASIA", "Hong Kong": "ASIA", "Israel": "ASIA", "Japan": "ASIA",
        "Republic of Korea": "ASIA", "Russia": "ASIA", "Singapore": "ASIA", "Thailand": "ASIA",
        "Albania": "EMEA", "Austria": "EMEA", "Belgium": "EMEA", "Bulgaria": "EMEA",
        "Croatia": "EMEA", "Czech Republic": "EMEA", "Finland": "EMEA", "France": "EMEA",
        "Germany": "EMEA", "Ireland": "EMEA", "Italy": "EMEA", "Latvia": "EMEA",
        "Luxembourg": "EMEA", "Netherlands": "EMEA", "Norway": "EMEA", "Poland": "EMEA",
        "Portugal": "EMEA", "Republic of Lithuania": "EMEA", "Romania": "EMEA",
        "Slovak Republic": "EMEA", "South Africa": "EMEA", "Spain": "EMEA", "Sweden": "EMEA",
        "Switzerland": "EMEA", "Ukraine": "EMEA", "United Kingdom": "EMEA",
        "Brazil": "AMER", "Canada": "AMER", "Chile": "AMER", "Mexico": "AMER",
        "Peru": "AMER", "United States": "AMER"
    }
    
    # Extract identity, country, and activated stake from the JSON data
    identity_info_map = {
        entry["identity"]: {
            "country": entry.get("ip_country"),
            "activated_stake": entry.get("activated_stake", 0.0)
        } 
        for entry in data
    }

    # PostgreSQL query to get block slot counts per identity for the given epoch
    query = """
    SELECT identity_pubkey, COUNT(block_slot)
    FROM leader_schedule
    WHERE epoch = %s
    GROUP BY identity_pubkey;
    """

    # Database connection
    from db_config import db_params
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Execute the query and fetch results
    cur.execute(query, (epoch,))
    block_slot_results = cur.fetchall()
    
    total_slots = sum(slot_count for _, slot_count in block_slot_results)
    total_activated_stake = 0  # To be accumulated from found identities
    
    # Initialize dictionaries to store results
    country_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0})
    region_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0})

    # Process results
    for identity_pubkey, slot_count in block_slot_results:
        identity_info = identity_info_map.get(identity_pubkey)
        if identity_info:
            country = identity_info["country"]
            activated_stake = identity_info["activated_stake"]
            total_activated_stake += activated_stake
            
            if country:
                # Update country results
                country_results[country]["count"] += 1
                country_results[country]["total_activated_stake"] += activated_stake
                country_results[country]["total_slots"] += slot_count
                country_results[country]["average_stake"] = country_results[country]["total_activated_stake"] / country_results[country]["count"]
                country_results[country]["percent_slots"] = round((country_results[country]["total_slots"] / total_slots) * 100, 2)

                # Determine the region
                region = country_to_region.get(country, "??")
                
                # Update region results
                region_results[region]["count"] += 1
                region_results[region]["total_activated_stake"] += activated_stake
                region_results[region]["total_slots"] += slot_count
                region_results[region]["average_stake"] = region_results[region]["total_activated_stake"] / region_results[region]["count"]
                region_results[region]["percent_slots"] = round((region_results[region]["total_slots"] / total_slots) * 100, 2)
        else:
            print(f"Error: Identity {identity_pubkey} not found in JSON data.")
    
    # Calculate percent_stake for each country and region
    for country in country_results:
        country_results[country]["percent_stake"] = round((country_results[country]["total_activated_stake"] / total_activated_stake) * 100, 2)

    for region in region_results:
        region_results[region]["percent_stake"] = round((region_results[region]["total_activated_stake"] / total_activated_stake) * 100, 2)
    
    # Convert results to dataframes and sort by total_slots
    country_df = pd.DataFrame.from_dict(country_results, orient='index')
    country_df.index.name = 'Country'
    country_df = country_df.sort_values(by="total_slots", ascending=False)
    country_df['total_activated_stake'] = country_df['total_activated_stake'].apply(lambda x: f"{x:,.2f}")
    country_df['total_slots'] = country_df['total_slots'].apply(lambda x: f"{x:,}")
    country_df['average_stake'] = country_df['average_stake'].apply(lambda x: f"{x:,.2f}")
    country_df['count'] = country_df['count'].apply(lambda x: f"{x:,}")

    region_df = pd.DataFrame.from_dict(region_results, orient='index')
    region_df.index.name = 'Region'
    region_df = region_df.sort_values(by="total_slots", ascending=False)
    region_df['total_activated_stake'] = region_df['total_activated_stake'].apply(lambda x: f"{x:,.2f}")
    region_df['total_slots'] = region_df['total_slots'].apply(lambda x: f"{x:,}")
    region_df['average_stake'] = region_df['average_stake'].apply(lambda x: f"{x:,.2f}")
    region_df['count'] = region_df['count'].apply(lambda x: f"{x:,}")

    # Add totals row to each dataframe
    country_totals = pd.DataFrame({
        'count': [f"{country_df['count'].apply(lambda x: int(x.replace(',', ''))).sum():,}"],
        'total_activated_stake': [f"{country_df['total_activated_stake'].apply(lambda x: float(x.replace(',', ''))).sum():,.2f}"],
        'total_slots': [f"{country_df['total_slots'].apply(lambda x: int(x.replace(',', ''))).sum():,}"],
        'average_stake': [f"{country_df['average_stake'].apply(lambda x: float(x.replace(',', ''))).mean():,.2f}"],
        'percent_slots': [round(country_df['percent_slots'].sum(), 2)],
        'percent_stake': [round(country_df['percent_stake'].sum(), 2)]
    }, index=['Total'])
    
    country_df = pd.concat([country_df, country_totals])

    region_totals = pd.DataFrame({
        'count': [f"{region_df['count'].apply(lambda x: int(x.replace(',', ''))).sum():,}"],
        'total_activated_stake': [f"{region_df['total_activated_stake'].apply(lambda x: float(x.replace(',', ''))).sum():,.2f}"],
        'total_slots': [f"{region_df['total_slots'].apply(lambda x: int(x.replace(',', ''))).sum():,}"],
        'average_stake': [f"{region_df['average_stake'].apply(lambda x: float(x.replace(',', ''))).mean():,.2f}"],
        'percent_slots': [round(region_df['percent_slots'].sum(), 2)],
        'percent_stake': [round(region_df['percent_stake'].sum(), 2)]
    }, index=['Total'])
    
    region_df = pd.concat([region_df, region_totals])
    
    # Adjust percentages to ensure they sum to 100% if slightly off
    if country_df['percent_stake']['Total'] < 100.0:
        diff = 100.0 - country_df['percent_stake']['Total']
        country_df.at[country_df.index[0], 'percent_stake'] += diff
    
    if region_df['percent_stake']['Total'] < 100.0:
        diff = 100.0 - region_df['percent_stake']['Total']
        region_df.at[region_df.index[0], 'percent_stake'] += diff
    
    # Generate pie charts
    country_df_for_pie = country_df[:-1]  # Exclude the totals row
    region_df_for_pie = region_df[:-1]  # Exclude the totals row

    # Country pie chart
    plt.figure(figsize=(10, 7))
    # Filter labels for countries with more than 3% stake
    labels = [
        f"{label} ({pct:.1f}%)" if pct >= 3 else ""
        for label, pct in zip(country_df_for_pie.index, country_df_for_pie['percent_stake'].astype(float))
    ]
     # Adding the "Powered By Trillium" title with images on both sides
    fig = plt.gcf()
    ax = plt.gca()

    # Load the image from the URL

    # Custom User-Agent header
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Create a request object with the headers
    url = "https://trillium.so/pages/favicon.ico"
    request = urllib.request.Request(url, headers=headers)

    # Open the URL using the request object
    with urllib.request.urlopen(request) as response:
        trillium_image = Image.open(response)

    # Convert the image to a numpy array and create the OffsetImage
    trillium_image = OffsetImage(np.array(trillium_image), zoom=0.05)
    
    # Create annotation boxes for the images (left and right)
    left_image = AnnotationBbox(trillium_image, (0.35, 1.06), frameon=False, url="https://trillium.so", xycoords='axes fraction')
    right_image = AnnotationBbox(trillium_image, (0.65, 1.06), frameon=False, url="https://trillium.so", xycoords='axes fraction')

    # Add the images to the plot
    ax.add_artist(left_image)
    ax.add_artist(right_image)

    # Add the "Powered By Trillium" title, centered and aligned with the images
    plt.text(0.5, 1.05, 'Powered By Trillium', ha='center', va='center', fontsize=12, weight='bold', transform=ax.transAxes)

    # Powered by Stakewiz logo setup
    url_stakewiz = "https://trillium.so/images/stakewiz-blk.png"
    request_stakewiz = urllib.request.Request(url_stakewiz, headers=headers)

    with urllib.request.urlopen(request_stakewiz) as response:
        stakewiz_image = Image.open(response)

    # Convert the image to RGB or RGBA format
    stakewiz_image = stakewiz_image.convert("RGBA")  # Ensure compatibility with Matplotlib

    # Convert the image to a numpy array and create the OffsetImage
    stakewiz_image = OffsetImage(np.array(stakewiz_image), zoom=.6)
    # Add the "Powered by Stakewiz" image at the bottom
    stakewiz_image_box = AnnotationBbox(stakewiz_image, (0.5, -0.015), frameon=False, xycoords='axes fraction')
    
    ax.add_artist(stakewiz_image_box)

    plt.pie(
        country_df_for_pie['percent_stake'].astype(float),
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%' if pct >= 3 else '',  # Only show percentages >= 3%
        startangle=140
    )
    plt.title(f"Percentage of Total Leader Slots / Activated Stake by Country - Epoch {epoch}")
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.savefig('country_pie_chart.png')
    plt.close()

    # Region pie chart
    plt.figure(figsize=(10, 7))
    # Filter labels for regions with more than 3% stake
    labels = [
        f"{label} ({pct:.1f}%)" if pct >= 3 else ""
        for label, pct in zip(region_df_for_pie.index, region_df_for_pie['percent_stake'].astype(float))
    ]

    # Adding the "Powered By Trillium" title with images on both sides
    fig = plt.gcf()
    ax = plt.gca()

    # Load the image from the URL

    # Custom User-Agent header
    headers = {'User-Agent': 'Mozilla/5.0'}

    # Create a request object with the headers
    url = "https://trillium.so/pages/favicon.ico"
    request = urllib.request.Request(url, headers=headers)

    # Open the URL using the request object
    with urllib.request.urlopen(request) as response:
        trillium_image = Image.open(response)

    # Convert the image to a numpy array and create the OffsetImage
    trillium_image = OffsetImage(np.array(trillium_image), zoom=0.04)
    
    # Create annotation boxes for the images (left and right)
    left_image = AnnotationBbox(trillium_image, (0.35, 1.06), frameon=False, url="https://trillium.so", xycoords='axes fraction')
    right_image = AnnotationBbox(trillium_image, (0.65, 1.06), frameon=False, url="https://trillium.so", xycoords='axes fraction')

    # Add the images to the plot
    ax.add_artist(left_image)
    ax.add_artist(right_image)

    # Add the "Powered By Trillium" title, centered and aligned with the images
    plt.text(0.5, 1.05, 'Powered By Trillium', ha='center', va='center', fontsize=12, weight='bold', transform=ax.transAxes)

    # Powered by Stakewiz logo setup
    url_stakewiz = "https://trillium.so/images/stakewiz-blk.png"
    request_stakewiz = urllib.request.Request(url_stakewiz, headers=headers)

    with urllib.request.urlopen(request_stakewiz) as response:
        stakewiz_image = Image.open(response)

    # Convert the image to RGB or RGBA format
    stakewiz_image = stakewiz_image.convert("RGBA")  # Ensure compatibility with Matplotlib

    # Convert the image to a numpy array and create the OffsetImage
    stakewiz_image = OffsetImage(np.array(stakewiz_image), zoom=0.6)
    # Add the "Powered by Stakewiz" image at the bottom
    stakewiz_image_box = AnnotationBbox(stakewiz_image, (0.5, -0.015), frameon=False, xycoords='axes fraction')

    ax.add_artist(stakewiz_image_box)

    plt.pie(
        region_df_for_pie['percent_stake'].astype(float),
        labels=labels,
        autopct=lambda pct: f'{pct:.1f}%' if pct >= 3 else '',  # Only show percentages >= 3%
        startangle=140
    )
    plt.title(f"Percentage of Total Leader Slots / Activated Stake by Region - Epoch {epoch}")
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.savefig('region_pie_chart.png')
    plt.close()

    return country_df, region_df

# Example usage
country_df, region_df = calculate_stake_statistics('validators-compact.json', 655)

# Display results
print("Country Results:")
print(country_df)
print("\nRegion Results:")
print(region_df)
