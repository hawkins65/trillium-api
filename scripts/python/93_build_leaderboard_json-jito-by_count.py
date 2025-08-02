# Standard library imports
import base64
import io
import json
import logging
import os
import random
import sys
from collections import defaultdict

# Third-party library imports
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
from plotly.subplots import make_subplots
import psycopg2

# PSQL Connection Configuration Parameters
from db_config import db_params

def format_lamports_to_sol(lamports, precision=7):
    from decimal import Decimal
    if lamports is None:
        return None
    sol_amount = Decimal(lamports) / Decimal('1000000000')
    if precision == 0:
        return int(sol_amount)
    return float(f"{sol_amount:.{precision}f}")

def format_number(number, precision):
    if number is None:
        return None
    return float(f"{number:.{precision}f}") if precision > 0 else int(number)

def get_min_max_epochs():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    query = "SELECT MIN(epoch), MAX(epoch) FROM validator_stats"
    cur.execute(query)
    result = cur.fetchone()
    min_epoch, max_epoch = result[0], result[1]
    cur.close()
    conn.close()
    return min_epoch, max_epoch

def get_color_map(items, filename='93_continent_colors.json'):
    # 7 colors for continents, no reds or browns, professional and pleasing
    colors = [
        '#4e79a7',  # Blue (e.g., Africa)
        '#f28e2b',  # Orange (e.g., Antarctica)
        '#76b7b2',  # Teal (e.g., Asia)
        '#59a14f',  # Green (e.g., Australia)
        '#edc948',  # Yellow (e.g., Europe)
        '#b07aa1',  # Purple (e.g., North America)
        '#17becf'   # Cyan (e.g., South America)
    ]
    color_map = {}
    
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            color_map = json.load(f)
    else:
        color_map = {}

    for item in items:
        if item not in color_map:
            if item == 'Unknown':
                color_map[item] = '#d3d3d3'  # Light gray
            else:
                available_colors = [c for c in colors if c not in color_map.values()]
                if not available_colors:
                    available_colors = colors
                color_map[item] = random.choice(available_colors)

    with open(filename, 'w') as f:
        json.dump(color_map, f)
    
    return color_map

def get_persistent_color_map(items, filename='93_country_colors.json'):
    # 30 colors for countries, no reds or grays, professional and pleasing
    colors = [
        '#1f77b4', '#ff7f0e', '#2ca02c', '#9467bd', '#8c564b',  # Blue, Orange, Green, Purple, Brown
        '#e377c2', '#bcbd22', '#17becf', '#aec7e8', '#ffbb78',  # Pink, Olive, Cyan, Light Blue, Peach
        '#98df8a', '#c5b0d5', '#c49c94', '#f7b6d2', '#dbdb8d',  # Light Green, Lavender, Tan, Light Pink, Beige
        '#9edae5', '#393b79', '#637939', '#8c6d31', '#7b4173',  # Aqua, Dark Blue, Olive Green, Dark Brown, Plum
        '#5254a3', '#8ca252', '#bd9e39', '#a55194', '#6b6ecf',  # Indigo, Sage, Golden Brown, Mauve, Periwinkle
        '#b5cf6b', '#e7ba52', '#9e9ac8', '#1abc9c', '#f1c40f'   # Lime, Gold, Lilac, Turquoise, Bright Yellow
    ]
    color_map = {}
    
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            color_map = json.load(f)
    else:
        color_map = {}

    for item in items:
        if item not in color_map:
            if item == 'Unknown':
                color_map[item] = '#d3d3d3'  # Light gray for Unknown (kept as exception)
            else:
                available_colors = [c for c in colors if c not in color_map.values()]
                if not available_colors:
                    available_colors = colors  # Recycle if needed
                color_map[item] = random.choice(available_colors)

    with open(filename, 'w') as f:
        json.dump(color_map, f)
    
    return color_map

def calculate_validator_counts(epoch, rank_range):
    try:
        if rank_range == '1-200':
            query = """
            SELECT vote_account_pubkey, city, country, continent, region,
                   delegator_compound_mev_apy, delegator_compound_total_apy, activated_stake
            FROM validator_stats
            WHERE epoch = %s
                AND jito_steward_overall_rank BETWEEN 1 AND 200
                AND city IS NOT NULL
                AND city != '';
            """
            rank_suffix = '-200'
            top_count = 200
        elif rank_range == '1-350':
            query = """
            SELECT vote_account_pubkey, city, country, continent, region,
                   delegator_compound_mev_apy, delegator_compound_total_apy, activated_stake
            FROM validator_stats
            WHERE epoch = %s
                AND jito_steward_overall_rank BETWEEN 1 AND 350
                AND city IS NOT NULL
                AND city != '';
            """
            rank_suffix = '-350'
            top_count = 350
        else:
            raise ValueError(f"Unexpected rank range: {rank_range}")

        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(query, (epoch,))
        validator_stats = cur.fetchall()
        cur.close()
        conn.close()

        country_counts = defaultdict(int)
        continent_counts = defaultdict(int)
        mev_apy_stake_products = []
        total_apy_stake_products = []
        mev_apy_values = []
        total_stake = 0

        for vote_account_pubkey, city, country, continent, region, mev_apy, total_apy, stake in validator_stats:
            if not country:
                country = "Unknown"
            if not continent:
                continent = "Unknown"
            country_counts[country] += 1
            continent_counts[continent] += 1
            if mev_apy is not None and stake is not None and stake > 0:
                mev_apy_decimal = mev_apy / 100  # Convert from percentage to decimal
                mev_apy_stake_products.append(mev_apy_decimal * stake)
                mev_apy_values.append(mev_apy_decimal)
                if mev_apy_decimal > 0.1:  # Log APYs > 10% after conversion
                    logging.info(f"High MEV APY: {mev_apy_decimal * 100:.2f}% for validator {vote_account_pubkey}, stake: {format_lamports_to_sol(stake, 2)} SOL, city: {city}")
            if total_apy is not None and stake is not None and stake > 0:
                total_apy_decimal = total_apy / 100  # Convert from percentage to decimal
                total_apy_stake_products.append(total_apy_decimal * stake)
            if stake is not None and stake > 0:
                total_stake += stake

        # Calculate stake-weighted averages
        avg_mev_apy = sum(mev_apy_stake_products) / total_stake if total_stake > 0 and mev_apy_stake_products else 0
        avg_total_apy = sum(total_apy_stake_products) / total_stake if total_stake > 0 and total_apy_stake_products else 0
        median_mev_apy = np.median(mev_apy_values) if mev_apy_values else 0
        avg_mev_apy_formatted = format_number(avg_mev_apy * 100, 2)  # Convert back to percentage for display
        avg_total_apy_formatted = format_number(avg_total_apy * 100, 2)
        median_mev_apy_formatted = format_number(median_mev_apy * 100, 2)

        country_df = pd.DataFrame.from_dict(country_counts, orient='index', columns=['count'])
        continent_df = pd.DataFrame.from_dict(continent_counts, orient='index', columns=['count'])

        total_country = country_df['count'].sum()
        country_df['percent'] = (country_df['count'] / total_country) * 100 if total_country else 0

        total_continent = continent_df['count'].sum()
        continent_df['percent'] = (continent_df['count'] / total_continent) * 100 if total_continent else 0

        country_df = country_df.sort_values(by='count', ascending=False)
        continent_df = continent_df.sort_values(by='count', ascending=False)

        countries = sorted(country_df.index)
        continents = sorted(continent_df.index)
        country_color_map = get_persistent_color_map(countries)
        continent_color_map = get_color_map(continents, 'continent_colors.json')

        # Calculate total counts
        total_continents = len(continent_df)
        total_countries = len(country_df)

        # Define titles and subtitle
        country_main_title = f"Count of Jito Validators by Country - Epoch {epoch} - Top {top_count}"
        continent_title = f"Total Continents: {total_continents}"
        country_title = f"Total Countries: {total_countries}"
        subtitle = f"Stake-Weighted -->  Avg MEV APY: {avg_mev_apy_formatted}% | Median MEV APY: {median_mev_apy_formatted}% | Avg Total APY: {avg_total_apy_formatted}%"

        # Create figure with subplots
        fig = make_subplots(rows=1, cols=2, specs=[[{'type': 'pie'}, {'type': 'pie'}]])

        # Continent Pie Chart
        continent_labels = continent_df.index
        continent_sizes = continent_df['count']
        continent_custom_labels = [f"{label} ({count:,} - {percent:.1f}%)" 
                                  for label, count, percent in zip(continent_labels, continent_df['count'], continent_df['percent'])]
        continent_colors = [continent_color_map[label] for label in continent_labels]

        fig.add_trace(
            go.Pie(
                labels=continent_labels,
                values=continent_sizes,
                text=continent_custom_labels,
                textposition='inside',
                textinfo='text',
                customdata=continent_df[['percent']],
                hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percent: %{customdata[0]:.1f}%',
                pull=[0.05] * len(continent_labels),
                marker=dict(colors=continent_colors),
                textfont=dict(size=10, color='white')
            ),
            row=1, col=1
        )

        # Country Pie Chart
        country_labels = country_df.index
        country_sizes = country_df['count']
        country_custom_labels = [f"{label} ({count:,} - {percent:.1f}%)" 
                                 for label, count, percent in zip(country_labels, country_df['count'], country_df['percent'])]
        country_colors = [country_color_map[label] for label in country_labels]

        fig.add_trace(
            go.Pie(
                labels=country_labels,
                values=country_sizes,
                text=country_custom_labels,
                textposition='outside',
                textinfo='text',
                customdata=country_df[['percent']],
                hovertemplate='<b>%{label}</b><br>Count: %{value:,}<br>Percent: %{customdata[0]:.1f}%',
                pull=[0.05] * len(country_labels),
                marker=dict(colors=country_colors),
                textfont=dict(size=10, color='black')
            ),
            row=1, col=2
        )

        # Update layout with centered main title, chart titles, subtitle, and clickable link annotation
        fig.update_layout(
            margin=dict(t=300, b=150, l=50, r=50),
            showlegend=False,
            annotations=[
                # Centered main title
                dict(
                    text=country_main_title,
                    x=0.5, y=1.30,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=20, color="black", family="sans-serif", weight="bold"),
                    align="center"
                ),
                # Single subtitle below main title
                dict(
                    text=subtitle,
                    x=0.5, y=1.20,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=12, color="black", family="sans-serif"),
                    align="center"
                ),
                # Continent title
                dict(
                    text=continent_title,
                    x=0.25, y=1.10,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=16, color="black", family="sans-serif"),
                    align="center"
                ),
                # Country title
                dict(
                    text=country_title,
                    x=0.75, y=1.10,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=16, color="black", family="sans-serif"),
                    align="center"
                ),
                # Clickable Trillium link below image
                dict(
                    text='<a href="https://trillium.so" target="_blank" style="color:blue; text-decoration:underline; font-size:10px; font-family:sans-serif;">https://trillium.so</a>',
                    x=0.5, y=0.02,
                    xref="paper", yref="paper",
                    showarrow=False,
                    font=dict(size=10, color="black", family="sans-serif"),  # Font fallback for non-HTML rendering
                    align="center"
                )
            ],
            images=[dict(
                source="https://trillium.so/images/fueled-by-trillium.png",
                xref="paper", yref="paper",
                x=0.5, y=0.08,
                sizex=0.1, sizey=0.033,
                xanchor="center", yanchor="top"
            )],
            title=None,
            title_text=""
        )

        # Save with favicon and custom HTML title
        epoch_filename = f'epoch{epoch}_validator_counts_charts-jito{rank_suffix}'
        fig.write_html(
            f"{epoch_filename}.html",
            include_plotlyjs='cdn',
            config={'responsive': True},
            full_html=True,
            post_script=f"""
            var head = document.getElementsByTagName('head')[0];
            var title = document.createElement('title');
            title.innerText = 'Jito Validator Counts - Top {top_count}';
            head.appendChild(title);
            var link = document.createElement('link');
            link.rel = 'icon';
            link.type = 'image/png';
            link.href = 'https://trillium.so/images/trillium-3d-no-leaves.png';
            head.appendChild(link);
            """
        )
        print(f"Combined chart saved as {epoch_filename}.html")

        return country_df, continent_df

    except Exception as e:
        print(f"Error in calculate_validator_counts for epoch {epoch} and rank {rank_range}:")
        print(f"Error message: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def main(epoch=None):
    min_epoch, max_epoch = get_min_max_epochs()
    if epoch is None:
        print("\nAvailable epoch range:")
        print(f"Minimum epoch: {min_epoch}")
        print(f"Maximum epoch: {max_epoch}")
        print()
        while True:
            epoch_input = input(f"Enter epoch (between {min_epoch} and {max_epoch}): ").strip()
            try:
                epoch = int(epoch_input)
                if min_epoch <= epoch <= max_epoch:
                    break
                else:
                    print(f"Please enter a value between {min_epoch} and {max_epoch}.")
            except ValueError:
                print("Please enter a valid integer.")
    else:
        if not (min_epoch <= epoch <= max_epoch):
            print(f"Error: Epoch {epoch} is outside the valid range ({min_epoch} to {max_epoch}).")
            sys.exit(1)

    for rank_range in ['1-200', '1-350']:
        print(f"Processing epoch: {epoch} for rank range {rank_range}")
        try:
            country_df, continent_df = calculate_validator_counts(epoch, rank_range)
            country_df.to_csv(f'epoch{epoch}_country_counts-jito{rank_range.replace("-", "_")}.csv', index=True)
            continent_df.to_csv(f'epoch{epoch}_continent_counts-jito{rank_range.replace("-", "_")}.csv', index=True)
        except Exception as e:
            logging.error(f"Failed to process epoch {epoch} for rank {rank_range}: {str(e)}")
    print("Processing complete.")

if __name__ == '__main__':
    if len(sys.argv) > 1:
        try:
            epoch = int(sys.argv[1])
            main(epoch)
        except ValueError:
            print("Error: Please provide a valid integer epoch as a command-line argument.")
            main()
    else:
        main()