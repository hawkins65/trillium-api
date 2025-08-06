# Log version immediately to confirm file loading
import os
import sys
import traceback
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from sqlalchemy import text
import pandas as pd
from collections import defaultdict

# Import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Import from current package
from .utils import format_lamports_to_sol, format_number, ensure_directory, get_output_path
from .visualizations import get_persistent_color_map, get_color_map

# Initialize logger
logger = setup_logging('stake_statistics')
logger.info("Loaded stake_statistics.py version 2025-07-21-2")

def calculate_stake_statistics(epoch, max_epoch, engine):
    try:
        query = """
        SELECT 
            identity_pubkey,
            activated_stake,
            country,
            continent,
            region
        FROM validator_stats
        WHERE epoch = :epoch
            AND activated_stake != 0;
        """
        slot_query = """
        SELECT identity_pubkey, COUNT(block_slot)
        FROM leader_schedule
        WHERE epoch = :epoch
        GROUP BY identity_pubkey;
        """
        with engine.connect() as conn:
            logger.info(f"validator_stats for epoch {epoch} query")
            validator_stats = conn.execute(text(query), {"epoch": epoch}).fetchall()
            logger.info(f"block_slot_results for epoch {epoch} slot_query")
            block_slot_results = conn.execute(text(slot_query), {"epoch": epoch}).fetchall()

        total_slots = sum(slot_count for _, slot_count in block_slot_results)
        logger.info(f"total_slots for epoch {epoch} {total_slots}")
        if total_slots == 0:
            logger.warning(f"total_slots is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_slots = 1
        total_activated_stake = sum(stats[1] for stats in validator_stats if stats[1] is not None)
        logger.info(f"total_activated_stake for epoch {epoch} {total_activated_stake}")
        if total_activated_stake == 0:
            logger.warning(f"total_activated_stake is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_activated_stake = 1

        slot_counts = dict(block_slot_results)
        country_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        continent_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        region_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})

        for identity_pubkey, activated_stake, country, continent, region in validator_stats:
            slot_count = slot_counts.get(identity_pubkey, 0)
            if activated_stake is None:
                activated_stake = 0.0
            if country:
                country_results[country]["count"] += 1
                country_results[country]["total_activated_stake"] += activated_stake
                country_results[country]["total_slots"] += slot_count
            if continent:
                continent_results[continent]["count"] += 1
                continent_results[continent]["total_activated_stake"] += activated_stake
                continent_results[continent]["total_slots"] += slot_count
            if region:
                region_results[region]["count"] += 1
                region_results[region]["total_activated_stake"] += activated_stake
                region_results[region]["total_slots"] += slot_count

        for results in [country_results, continent_results, region_results]:
            for area in results:
                if results[area]["count"] > 0:
                    results[area]["average_stake"] = results[area]["total_activated_stake"] / results[area]["count"]
                else:
                    results[area]["average_stake"] = 0.0
                results[area]["percent_stake"] = (results[area]["total_activated_stake"] / total_activated_stake) * 100 if total_activated_stake > 0 else 0.0
                results[area]["percent_slots"] = (results[area]["total_slots"] / total_slots) * 100 if total_slots > 0 else 0.0

        country_df = pd.DataFrame.from_dict(country_results, orient='index')
        continent_df = pd.DataFrame.from_dict(continent_results, orient='index')
        region_df = pd.DataFrame.from_dict(region_results, orient='index')

        for df_name, df in [("country_df", country_df), ("continent_df", continent_df), ("region_df", region_df)]:
            if df.empty:
                logger.warning(f"{df_name} is empty for epoch {epoch}")
                df = pd.DataFrame({
                    'count': [0], 'total_activated_stake': [0.0], 'total_slots': [0],
                    'average_stake': [0.0], 'percent_stake': [0.0], 'percent_slots': [0.0]
                }, index=['Unknown'])
                if df_name == "country_df":
                    country_df = df
                elif df_name == "continent_df":
                    continent_df = df
                else:
                    region_df = df

        for df in [country_df, continent_df, region_df]:
            df.index.name = 'Area'
            if 'total_slots' in df.columns:
                df = df.sort_values(by="total_slots", ascending=False)
            else:
                logger.warning(f"'total_slots' column not found in DataFrame for epoch {epoch}")
            if 'total_activated_stake' in df.columns:
                df['total_activated_stake'] = df['total_activated_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'total_slots' in df.columns:
                df['total_slots'] = df['total_slots'].apply(lambda x: f"{x:,}" if x is not None else "0")
            if 'average_stake' in df.columns:
                df['average_stake'] = df['average_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'count' in df.columns:
                df['count'] = df['count'].apply(lambda x: f"{x:,}" if x is not None else "0")
            if 'percent_stake' in df.columns:
                df['percent_stake'] = df['percent_stake'].apply(lambda x: round(x, 2) if x is not None else 0.0)
            if 'percent_slots' in df.columns:
                df['percent_slots'] = df['percent_slots'].apply(lambda x: round(x, 2) if x is not None else 0.0)

        countries = sorted(set(country_df.index))
        continents = sorted(set(continent_df.index))
        regions = sorted(set(region_df.index))

        try:
            country_color_map = get_persistent_color_map(countries)
            continent_color_map = get_color_map(continents)
            region_color_map = get_color_map(regions)
        except Exception as e:
            logger.error(f"Error creating color maps for epoch {epoch}: {str(e)}")
            country_color_map = {'Unknown': '#CCCCCC'}
            continent_color_map = {'Unknown': '#CCCCCC'}
            region_color_map = {'Unknown': '#CCCCCC'}

        def create_pie_chart(df, title, color_map, subplot_col):
            try:
                if 'percent_stake' not in df.columns:
                    logger.warning(f"'percent_stake' column not found in DataFrame for {title}")
                    return
                
                df_for_pie = df[df['percent_stake'] >= 0.0].sort_values(by='percent_stake', ascending=False)
                if df_for_pie.empty:
                    logger.warning(f"No data with percent_stake >= 0.0 for {title}")
                    return
                
                labels = df_for_pie.index
                sizes = df_for_pie['percent_stake']
                
                for label in labels:
                    if label not in color_map:
                        color_map[label] = '#CCCCCC'
                
                colors = [color_map[label] for label in labels]
                custom_labels = [f"{label} ({size:.2f}%)" for label, size in zip(labels, sizes)]

                text_position = 'inside' if subplot_col == 1 else 'outside'
                text_font = dict(size=10, color='white' if subplot_col == 1 else 'black')

                fig.add_trace(go.Pie(
                    values=sizes,
                    labels=labels,
                    text=custom_labels,
                    textposition=text_position,
                    textinfo='text',
                    hovertemplate='<b>%{label}</b><br>Percent Stake: %{value:.5f}%<br>Total Stake: %{customdata[0]:.0f} SOL<br>Total Slots: %{customdata[1]}',
                    customdata=df_for_pie[['total_activated_stake', 'total_slots']],
                    pull=[0.05] * len(labels),
                    marker=dict(colors=colors),
                    textfont=text_font
                ), row=1, col=subplot_col)
            except Exception as e:
                logger.error(f"Error creating pie chart for {title}: {str(e)}")
                logger.error(f"Pie chart traceback: {traceback.format_exc()}")

        try:
            fig = make_subplots(rows=1, cols=2, 
                                specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                                subplot_titles=(f"Stake by Continent - Epoch {epoch}", f"Stake by Country - Epoch {epoch}"),
                                horizontal_spacing=0.1)

            create_pie_chart(continent_df, f"Stake by Continent - Epoch {epoch}", continent_color_map, 1)
            create_pie_chart(country_df, f"Stake by Country - Epoch {epoch}", country_color_map, 2)

            fig.update_layout(
                showlegend=False,
                title=dict(
                    text=f"Stake by Continent and Country - Epoch {epoch}",
                    y=0.95, 
                    x=0.5, 
                    xanchor='center', 
                    font=dict(size=16, weight='bold')
                ),
                height=900,
                width=1200,
                barmode='group',
                legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
                margin=dict(t=150, b=250, l=50, r=50)
            )

            from .utils import save_chart_html
            filename = get_output_path(f'epoch{epoch}_stake_distribution_charts.html', 'html')
            save_chart_html(fig, "Solana Stake Distribution Charts", filename)

            if epoch == max_epoch:
                filename = get_output_path('stake_distribution_charts.html', 'html')
                save_chart_html(fig, "Solana Stake Distribution Charts", filename)

        except Exception as e:
            logger.error(f"Error creating or saving figure for epoch {epoch}: {str(e)}")
            logger.error(f"Figure traceback: {traceback.format_exc()}")

        try:
            country_df.to_csv(get_output_path(f'epoch{epoch}_country_stats.csv', 'csv'), index=True)
            logger.info(f"country_df epoch{epoch}_country_stats.csv")
        except Exception as e:
            logger.error(f"Error writing country_df to CSV for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(get_output_path(f'epoch{epoch}_country_stats.csv', 'csv'))

        try:
            continent_df.to_csv(get_output_path(f'epoch{epoch}_continent_stats.csv', 'csv'), index=True)
            logger.info(f"continent_df epoch820_continent_stats.csv")
        except Exception as e:
            logger.error(f"Error writing continent_df to CSV for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(get_output_path(f'epoch{epoch}_continent_stats.csv', 'csv'))

        try:
            region_df.to_csv(get_output_path(f'epoch{epoch}_region_stats.csv', 'csv'), index=True)
            logger.info(f"region_df epoch{epoch}_region_stats.csv")
        except Exception as e:
            logger.error(f"Error writing region_df to CSV for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(get_output_path(f'epoch{epoch}_region_stats.csv', 'csv'))

        return country_df, continent_df, region_df

    except Exception as e:
        logger.error(f"Error in calculate_stake_statistics for epoch {epoch}: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")

        empty_df = pd.DataFrame({
            'count': [0], 'total_activated_stake': [0], 'total_slots': [0],
            'average_stake': [0], 'percent_stake': [0], 'percent_slots': [0]
        }, index=['Unknown'])
        empty_df.index.name = 'Area'

        empty_df.to_csv(get_output_path(f'epoch{epoch}_country_stats.csv', 'csv'), index=True)
        empty_df.to_csv(get_output_path(f'epoch{epoch}_continent_stats.csv', 'csv'), index=True)
        empty_df.to_csv(get_output_path(f'epoch{epoch}_region_stats.csv', 'csv'), index=True)

        return empty_df.copy(), empty_df.copy(), empty_df.copy()

def calculate_stake_statistics_metro(epoch, max_epoch, engine):
    logger.debug(f"Entering calculate_stake_statistics_metro for epoch {epoch}, version 2025-07-21-2")
    try:
        logger.debug(f"os module available: {os.__name__}")

        # Initialize DataFrames to avoid NameError
        country_df = pd.DataFrame({
            'count': [0], 
            'total_activated_stake': [0], 
            'total_slots': [0], 
            'average_stake': [0], 
            'percent_stake': [0], 
            'percent_slots': [0]
        }, index=['Unknown'])
        country_df.index.name = 'Area'
        metro_df = pd.DataFrame({
            'count': [0], 
            'total_activated_stake': [0], 
            'total_slots': [0], 
            'average_stake': [0], 
            'percent_stake': [0], 
            'percent_slots': [0]
        }, index=['Unknown'])
        metro_df.index.name = 'Area'

        logger.debug("Checking validator_stats schema")
        with engine.connect() as conn:
            try:
                schema_query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'validator_stats' AND column_name = 'metro';
                """
                metro_column_exists = conn.execute(text(schema_query)).fetchone()
                logger.debug(f"Metro column exists: {bool(metro_column_exists)}")
            except Exception as schema_e:
                logger.error(f"Failed to check validator_stats schema: {str(schema_e)}")
                metro_column_exists = None

        query_columns = ["identity_pubkey", "activated_stake", "country", "continent", "region"]
        if metro_column_exists:
            query_columns.insert(2, "metro")
        query = f"""
        SELECT 
            {', '.join(query_columns)}
        FROM validator_stats
        WHERE epoch = :epoch
            AND activated_stake != 0;
        """

        logger.debug("Executing validator_stats query")
        with engine.connect() as conn:
            try:
                validator_stats = conn.execute(text(query), {"epoch": epoch}).fetchall()
                logger.debug(f"validator_stats query returned {len(validator_stats)} rows")
            except Exception as query_e:
                logger.error(f"Failed to execute validator_stats query for epoch {epoch}: {str(query_e)}")
                raise

            logger.debug("Executing slot_query")
            slot_query = """
            SELECT identity_pubkey, COUNT(block_slot)
            FROM leader_schedule
            WHERE epoch = :epoch
            GROUP BY identity_pubkey;
            """
            try:
                block_slot_results = conn.execute(text(slot_query), {"epoch": epoch}).fetchall()
                logger.debug(f"block_slot_results query returned {len(block_slot_results)} rows")
            except Exception as query_e:
                logger.error(f"Failed to execute slot_query for epoch {epoch}: {str(query_e)}")
                raise

        logger.debug("Calculating total_slots")
        total_slots = sum(slot_count for _, slot_count in block_slot_results)
        logger.info(f"total_slots for epoch {epoch} {total_slots}")
        if total_slots == 0:
            logger.warning(f"total_slots is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_slots = 1
            
        logger.debug("Calculating total_activated_stake")
        total_activated_stake = sum(stats[1] for stats in validator_stats if stats[1] is not None)
        logger.info(f"total_activated_stake for epoch {epoch} {total_activated_stake}")
        if total_activated_stake == 0:
            logger.warning(f"total_activated_stake is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_activated_stake = 1
        
        logger.debug("Creating slot_counts dictionary")
        slot_counts = dict(block_slot_results)

        logger.debug("Processing validator_stats")
        country_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        metro_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})

        for row in validator_stats:
            identity_pubkey = row[0]
            activated_stake = row[1]
            metro = row[2] if metro_column_exists else None
            country = row[2 + (1 if metro_column_exists else 0)]
            continent = row[3 + (1 if metro_column_exists else 0)]
            region = row[4 + (1 if metro_column_exists else 0)]
            slot_count = slot_counts.get(identity_pubkey, 0)
            if activated_stake is None:
                activated_stake = 0.0
            if country:
                country_results[country]["count"] += 1
                country_results[country]["total_activated_stake"] += activated_stake
                country_results[country]["total_slots"] += slot_count
            if metro:
                metro_results[metro]["count"] += 1
                metro_results[metro]["total_activated_stake"] += activated_stake
                metro_results[metro]["total_slots"] += slot_count

        logger.debug("Calculating statistics")
        for results in [country_results, metro_results]:
            for area in results:
                if results[area]["count"] > 0:
                    results[area]["average_stake"] = results[area]["total_activated_stake"] / results[area]["count"]
                else:
                    results[area]["average_stake"] = 0.0
                results[area]["percent_stake"] = (results[area]["total_activated_stake"] / total_activated_stake) * 100 if total_activated_stake > 0 else 0.0
                results[area]["percent_slots"] = (results[area]["total_slots"] / total_slots) * 100 if total_slots > 0 else 0.0

        logger.debug("Creating DataFrames")
        country_df = pd.DataFrame.from_dict(country_results, orient='index')
        metro_df = pd.DataFrame.from_dict(metro_results, orient='index')
        
        logger.debug("Checking for empty DataFrames")
        for df_name, df in [("country_df", country_df), ("metro_df", metro_df)]:
            if df.empty:
                logger.warning(f"{df_name} is empty for epoch {epoch}")
                df = pd.DataFrame({
                    'count': [0], 
                    'total_activated_stake': [0.0], 
                    'total_slots': [0], 
                    'average_stake': [0.0], 
                    'percent_stake': [0.0], 
                    'percent_slots': [0.0]
                }, index=['Unknown'])
                if df_name == "country_df":
                    country_df = df
                else:
                    metro_df = df

        logger.debug("Formatting DataFrames")
        for df in [country_df, metro_df]:
            df.index.name = 'Area'
            if 'total_slots' in df.columns:
                df = df.sort_values(by="total_slots", ascending=False)
            else:
                logger.warning(f"'total_slots' column not found in DataFrame for epoch {epoch}")
            
            if 'total_activated_stake' in df.columns:
                df['total_activated_stake'] = df['total_activated_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'total_slots' in df.columns:
                df['total_slots'] = df['total_slots'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            if 'average_stake' in df.columns:
                df['average_stake'] = df['average_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'count' in df.columns:
                df['count'] = df['count'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            if 'percent_stake' in df.columns:
                df['percent_stake'] = df['percent_stake'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)
            if 'percent_slots' in df.columns:
                df['percent_slots'] = df['percent_slots'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)

        logger.debug("Creating color maps")
        countries = sorted(set(country_df.index))
        metros = sorted(set(metro_df.index))

        try:
            country_color_map = get_persistent_color_map(countries)
            metro_color_map = get_color_map(metros)
        except Exception as e:
            logger.error(f"Error creating color maps for epoch {epoch}: {str(e)}")
            country_color_map = {'Unknown': '#CCCCCC'}
            metro_color_map = {'Unknown': '#CCCCCC'}

        def create_pie_chart(df, title, color_map, subplot_col, limit=None):
            try:
                if 'percent_stake' not in df.columns:
                    logger.warning(f"'percent_stake' column not found in DataFrame for {title}")
                    return
                
                df_for_pie = df[df['percent_stake'] >= 0.5].sort_values(by='percent_stake', ascending=False) if subplot_col == 1 else df.sort_values(by='percent_stake', ascending=False).head(30)
                if df_for_pie.empty:
                    logger.warning(f"No data with percent_stake >= 0.5 for {title}")
                    return
                
                labels = df_for_pie.index
                sizes = df_for_pie['percent_stake']
                
                for label in labels:
                    if label not in color_map:
                        color_map[label] = '#CCCCCC'
                
                colors = [color_map[label] for label in labels]
                custom_labels = [f"{label} ({size:.1f}%)" for label, size in zip(labels, sizes)]

                text_position = 'inside' if subplot_col == 1 else 'outside'
                text_font = dict(size=10, color='white' if subplot_col == 1 else 'black')

                fig.add_trace(go.Pie(
                    values=sizes,
                    labels=labels,
                    text=custom_labels,
                    textposition=text_position,
                    textinfo='text',
                    hovertemplate='<b>%{label}</b><br>Percent Stake: %{value:.1f}%<br>Total Stake: %{customdata[0]:.0f} SOL<br>Total Slots: %{customdata[1]}',
                    customdata=df_for_pie[['total_activated_stake', 'total_slots']],
                    pull=[0.05] * len(labels),
                    marker=dict(colors=colors),
                    textfont=text_font
                ), row=1, col=subplot_col)
            except Exception as e:
                logger.error(f"Error creating pie chart for {title}: {str(e)}")
                logger.error(f"Pie chart traceback: {traceback.format_exc()}")

        logger.debug("Creating pie chart figure")
        try:
            fig = make_subplots(rows=1, cols=2, 
                                specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                                subplot_titles=(f"Stake by Country - Epoch {epoch}", f"Stake by Metro (Top 30) - Epoch {epoch}"),
                                horizontal_spacing=0.1)

            create_pie_chart(country_df, f"Stake by Country - Epoch {epoch}", country_color_map, 1)
            create_pie_chart(metro_df, f"Stake by Metro - Epoch {epoch}", metro_color_map, 2, limit=30)

            fig.update_layout(
                showlegend=False,
                title=dict(
                    text=f"Stake by Country and Metro (Top 30) - Epoch {epoch}",
                    y=0.95, 
                    x=0.5, 
                    xanchor='center', 
                    font=dict(size=16, weight='bold')
                ),
                height=900,
                width=1200,
                barmode='group',
                legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
                margin=dict(t=150, b=250, l=50, r=50)
            )

            from .utils import save_chart_html
            logger.debug(f"Saving HTML chart to epoch{epoch}_stake_distribution_charts_metro.html")
            filename = get_output_path(f'epoch{epoch}_stake_distribution_charts_metro.html', 'html')
            save_chart_html(fig, "Solana Stake Distribution Charts (Metro)", filename)

            if epoch == max_epoch:
                logger.debug(f"Saving HTML chart to stake_distribution_charts_metro.html")
                filename = get_output_path('stake_distribution_charts_metro.html', 'html')
                save_chart_html(fig, "Solana Stake Distribution Charts (Metro)", filename)

        except Exception as e:
            logger.error(f"Error creating or saving figure for epoch {epoch}: {str(e)}")
            logger.error(f"Figure traceback: {traceback.format_exc()}")

        logger.debug("Writing CSV files")
        try:
            country_df.to_csv(get_output_path(f'epoch{epoch}_country_stats_metro.csv', 'csv'), index=True)
            logger.info(f"country_df epoch{epoch}_country_stats_metro.csv")
            metro_df.to_csv(get_output_path(f'epoch{epoch}_metro_stats_metro.csv', 'csv'), index=True)
            logger.info(f"metro_df epoch{epoch}_metro_stats_metro.csv")
        except Exception as e:
            logger.error(f"Error writing CSVs for epoch {epoch}: {e}")
            country_df.to_csv(get_output_path(f'epoch{epoch}_country_stats_metro.csv', 'csv'), index=True)
            metro_df.to_csv(get_output_path(f'epoch{epoch}_metro_stats_metro.csv', 'csv'), index=True)

        logger.debug("Returning DataFrames")
        return country_df, metro_df
    
    except Exception as e:
        logger.error(f"Error in calculate_stake_statistics_metro for epoch {epoch}: {str(e)}")
        logger.error(f"Exception details: {type(e).__name__}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        empty_df = pd.DataFrame({
            'count': [0], 
            'total_activated_stake': [0], 
            'total_slots': [0], 
            'average_stake': [0], 
            'percent_stake': [0], 
            'percent_slots': [0]
        }, index=['Unknown'])
        empty_df.index.name = 'Area'
        
        logger.debug("Writing fallback CSV files")
        try:
            empty_df.to_csv(get_output_path(f'epoch{epoch}_country_stats_metro.csv', 'csv'), index=True)
            empty_df.to_csv(get_output_path(f'epoch{epoch}_metro_stats_metro.csv', 'csv'), index=True)
        except Exception as csv_e:
            logger.error(f"Error writing fallback CSVs for epoch {epoch}: {csv_e}")
        
        return empty_df.copy(), empty_df.copy()