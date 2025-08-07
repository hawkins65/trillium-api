import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
import random
import os
import sys
import traceback

# Import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import from current package
from utils import save_chart_html, ensure_directory, get_output_path

# Initialize logger
logger = setup_logging('visualizations')

def get_color_map(items):
    colors = [
        '#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2', '#FF00FF', '#00FFFF',
        '#FF4500', '#32CD32', '#4169E1', '#FFD700', '#FF1493', '#40E0D0',
        '#DC143C', '#7FFF00', '#1E90FF', '#FFA500', '#EE82EE', '#00CED1',
        '#FF6347', '#3CB371', '#6495ED', '#DA70D6', '#48D1CC', '#66CDAA',
        '#87CEFA', '#FF69B4', '#20B2AA', '#BA55D3', '#5F9EA0', '#CD5C5C',
        '#FFB6C1', '#9370DB', '#7FFFD4', '#FF7F50', '#8A2BE2', '#8B008B',
        '#B22222', '#228B22', '#FF8C00', '#8B4513', '#7B68EE', '#6A5ACD',
        '#4682B4', '#DDA0DD', '#D2691E', '#B0E0E6', '#32CD32', '#ADFF2F',
        '#FFA07A', '#87CEEB', '#9370DB', '#B0C4DE', '#FFDEAD', '#F4A460',
        '#DAA520', '#A0522D', '#A52A2A', '#708090', '#556B2F', '#8FBC8F',
        '#CD853F', '#BC8F8F', '#2F4F4F', '#D3D3D3', '#00BFFF', '#8A2BE2'
    ]
    color_cycle = (colors * (len(items) // len(colors) + 1))[:len(items)]
    return dict(zip(sorted(items), color_cycle))

def get_persistent_color_map(items, filename='country_colors.json'):
    colors = [
        '#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2', '#FF00FF', '#00FFFF',
        '#FF4500', '#32CD32', '#4169E1', '#FFD700', '#FF1493', '#40E0D0',
        '#DC143C', '#7FFF00', '#1E90FF', '#FFA500', '#EE82EE', '#00CED1',
        '#FF6347', '#3CB371', '#6495ED', '#DA70D6', '#48D1CC', '#66CDAA',
        '#87CEFA', '#FF69B4', '#20B2AA', '#BA55D3', '#5F9EA0', '#CD5C5C',
        '#FFB6C1', '#9370DB', '#7FFFD4', '#FF7F50', '#8A2BE2', '#8B008B',
        '#B22222', '#228B22', '#FF8C00', '#8B4513', '#7B68EE', '#6A5ACD',
        '#4682B4', '#DDA0DD', '#D2691E', '#B0E0E6', '#32CD32', '#ADFF2F',
        '#FFA07A', '#87CEEB', '#9370DB', '#B0C4DE', '#FFDEAD', '#F4A460',
        '#DAA520', '#A0522D', '#A52A2A', '#708090', '#556B2F', '#8FBC8F',
        '#CD853F', '#BC8F8F', '#2F4F4F', '#D3D3D3', '#00BFFF', '#8A2BE2'
    ]

    color_map = {}
    filename = get_output_path(filename, 'json')
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            color_map = json.load(f)
    else:
        color_map = {}

    for item in items:
        if item not in color_map:
            if item == 'Unknown':
                color_map[item] = '#CCCCCC'
            else:
                available_colors = [c for c in colors if c not in color_map.values()]
                if not available_colors:
                    available_colors = colors
                color_map[item] = random.choice(available_colors)

    with open(filename, 'w') as f:
        json.dump(color_map, f)
    return color_map

def plot_votes_cast_metrics(epoch, max_epoch):
    file_path = get_output_path("last_ten_epoch_aggregate_data.json", 'json')
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        latest_epochs = data[::-1]
        epochs = [epoch['epoch'] for epoch in latest_epochs]
        avg_votes_cast = [epoch['avg_votes_cast'] for epoch in latest_epochs]
        median_votes_cast = [epoch['median_votes_cast'] for epoch in latest_epochs]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=epochs, y=avg_votes_cast, name="Average Votes Cast",
            marker_color='#6A9F4F', width=0.35, offset=-0.175,
            text=[f"{v:,.0f}" for v in avg_votes_cast],
            textposition='inside',
            textfont=dict(color="white"),
            textangle=-90
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=median_votes_cast, name="Median Votes Cast",
            marker_color='#4E89A7', width=0.35, offset=0.175,
            text=[f"{v:,.0f}" for v in median_votes_cast],
            textposition='inside',
            textfont=dict(color="white"),
            textangle=-90
        ))

        fig.update_layout(
            autosize=False,
            title=dict(
                text="Solana Epoch Votes Cast Metrics Overview",
                y=0.98,
                x=0.5,
                xanchor='center',
                font=dict(size=20, weight='bold')
            ),
            xaxis=dict(
                title=dict(
                    text="Epoch",
                    font=dict(size=16, weight='bold')
                ),
                title_standoff=3
            ),
            height=900,
            width=1200,
            barmode='group',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.05,
                xanchor="center",
                x=0.5,
                font=dict(size=12)
            ),
            margin=dict(t=100, b=150, l=50, r=50),
            uniformtext_minsize=10,
            uniformtext_mode='hide'
        )

        filename = f'epoch{epoch}_votes_cast_metrics_chart.html'
        save_chart_html(fig, "Solana Epoch Votes Cast Metrics Overview", filename)

        if epoch == max_epoch:
            filename = 'votes_cast_metrics_chart.html'
            save_chart_html(fig, "Solana Epoch Votes Cast Metrics Overview", filename)
        
    except FileNotFoundError:
        logger.error(f"Error: The file {file_path} was not found.")
    except json.JSONDecodeError:
        logger.error("Error: Could not decode JSON from the file.")

def plot_latency_and_consensus_charts(start_epoch, end_epoch, max_epoch, engine=None):
    from sqlalchemy import text
    if start_epoch == end_epoch:
        epoch_query = "SELECT DISTINCT epoch FROM validator_stats WHERE epoch <= :end_epoch ORDER BY epoch DESC LIMIT 10"
        with engine.connect() as conn:
            recent_epochs = pd.read_sql(text(epoch_query), engine, params={"end_epoch": end_epoch})
        epoch_range = recent_epochs['epoch'].tolist()
    else:
        epoch_range = list(range(start_epoch, end_epoch + 1))

    query = """
    SELECT 
        vs.epoch, 
        vs.version, 
        vs.activated_stake,
        vx.average_vl, 
        vx.average_llv, 
        vx.average_cv,
        vt.mean_vote_latency,
        vt.median_vote_latency,
        vt.vote_credits_rank,
        vt.avg_credit_per_voted_slot
    FROM validator_stats vs
    JOIN validator_xshin vx 
        ON vs.vote_account_pubkey = vx.vote_account_pubkey 
        AND vs.epoch = vx.epoch
    JOIN votes_table vt 
        ON vs.vote_account_pubkey = vt.vote_account_pubkey 
        AND vs.epoch = vt.epoch
    WHERE vs.epoch IN :epochs
    """
    with engine.connect() as conn:
        data = pd.read_sql(text(query), engine, params={"epochs": tuple(epoch_range)})

    data['version_digit'] = data['version'].apply(
        lambda x: re.match(r'[0-2]', str(x)).group() if re.match(r'[0-2]', str(x)) else None
    )
    data = data.dropna(subset=['version_digit'])
    data = data[data['version_digit'].isin(['0', '2'])]

    data['rank'] = data.groupby('epoch')['activated_stake'].rank(method='dense', ascending=False)
    data['is_top30'] = data.groupby('epoch')['rank'].transform(lambda x: x <= x.quantile(0.3))
    top30_data = data[(data['version_digit'] == '2') & (data['is_top30'])]

    v0_color = '#6A9F4F'
    v2_color = '#4E89A7'
    v2_top30_color = '#66B2FF'

    fig = make_subplots(rows=3, cols=1, vertical_spacing=0.1)

    latency_data = data.dropna(subset=['mean_vote_latency'])
    latency_data = latency_data[latency_data['mean_vote_latency'] > 0]
    grouped_latency_data = latency_data.groupby(['epoch', 'version_digit'])[['mean_vote_latency']].mean().reset_index()
    top30_latency = top30_data.dropna(subset=['mean_vote_latency'])
    top30_latency = top30_latency[top30_latency['mean_vote_latency'] > 0]
    top30_latency_data = top30_latency.groupby('epoch')[['mean_vote_latency']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_latency_data[grouped_latency_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_latency_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['mean_vote_latency'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                text=[f"{round(val, 2):,.2f}" for val in subset['mean_vote_latency']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

    llv_data = data.dropna(subset=['vote_credits_rank'])
    llv_data = llv_data[llv_data['vote_credits_rank'] > 0]
    grouped_llv_data = llv_data.groupby(['epoch', 'version_digit'])[['vote_credits_rank']].mean().reset_index()
    top30_llv = top30_data.dropna(subset=['vote_credits_rank'])
    top30_llv = top30_llv[top30_llv['vote_credits_rank'] > 0]
    top30_llv_data = top30_llv.groupby('epoch')[['vote_credits_rank']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_llv_data[grouped_llv_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_llv_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['vote_credits_rank'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                showlegend=False,
                text=[f"{round(val):,}" for val in subset['vote_credits_rank']],
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

    cv_data = data.dropna(subset=['average_cv'])
    cv_data = cv_data[cv_data['average_cv'] > 0]
    grouped_cv_data = cv_data.groupby(['epoch', 'version_digit'])[['average_cv']].mean().reset_index()
    top30_cv = top30_data.dropna(subset=['average_cv'])
    top30_cv = top30_cv[top30_cv['average_cv'] > 0]
    top30_cv_data = top30_cv.groupby('epoch')[['average_cv']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_cv_data[grouped_cv_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_cv_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['average_cv'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                showlegend=False,
                text=[f"{round(val * 100)}%" for val in subset['average_cv']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=3, col=1)

    fig.update_layout(
        title=dict(
            text="Solana Validator Client Version - Latency & Consensus",
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

    fig.update_xaxes(title_text="(smaller = better)", row=1, col=1)
    fig.update_xaxes(title_text="(smaller = better)", row=2, col=1)
    fig.update_xaxes(title_text="", row=3, col=1)
    fig.add_annotation(
        text="(larger = better)<br><b>Epoch</b>",
        xref="paper", yref="paper",
        x=0.5, y=-0.1,
        showarrow=False,
        font=dict(size=14),
        align="center"
    )
    
    fig.update_yaxes(title_text="Vote Latency (slots)", row=1, col=1)
    fig.update_yaxes(title_text="Vote Credits Rank", tickformat=",", row=2, col=1)
    fig.update_yaxes(title_text="Consensus Votes", tickformat=".0%", row=3, col=1)

    epoch = end_epoch
    filename = f'epoch{epoch}_latency_and_consensus_charts.html'
    save_chart_html(fig, "Solana Latency and Consensus Charts", filename)

    if epoch == max_epoch:
        filename = 'latency_and_consensus_charts.html'
        save_chart_html(fig, "Solana Latency and Consensus Charts", filename)

def plot_epoch_comparison_charts(start_epoch, end_epoch, max_epoch, engine=None):
    from sqlalchemy import text
    if start_epoch == end_epoch:
        epoch_query = "SELECT DISTINCT epoch FROM validator_stats WHERE epoch <= :end_epoch ORDER BY epoch DESC LIMIT 10"
        with engine.connect() as conn:
            recent_epochs = pd.read_sql(text(epoch_query), engine, params={"end_epoch": end_epoch})
        epoch_range = recent_epochs['epoch'].tolist()
    else:
        epoch_range = list(range(start_epoch, end_epoch + 1))

    query = """
        SELECT epoch, version, activated_stake,
            avg_priority_fees_per_block, avg_mev_per_block, avg_signature_fees_per_block,
            avg_cu_per_block, avg_user_tx_per_block, avg_vote_tx_per_block
        FROM validator_stats
        WHERE epoch IN :epochs
        """

    with engine.connect() as conn:
        data = pd.read_sql(text(query), engine, params={"epochs": tuple(epoch_range)})

    data['version_digit'] = data['version'].apply(
        lambda x: re.match(r'[0-2]', str(x)).group() if re.match(r'[0-2]', str(x)) else None
    )
    data = data.dropna(subset=['version_digit'])
    sample_size = data.groupby(['epoch', 'version_digit']).size().unstack(fill_value=0)

    DEBUG = False
    if DEBUG:
        print("The sample sizes by epoch are (Epoch, v0, v2):")
        for epoch in sample_size.index:
            v0 = sample_size.loc[epoch, '0'] if '0' in sample_size.columns else 0
            v2 = sample_size.loc[epoch, '2'] if '2' in sample_size.columns else 0
            print(f"{epoch}, {v0}, {v2}")

    data = data[data['version_digit'].isin(['0', '2'])]
    LAMPORTS_PER_SOL = 1_000_000_000

    v0_color = '#6A9F4F'
    v2_color = '#4E89A7'
    v2_top30_color = '#66B2FF'

    fig = make_subplots(rows=3, cols=1, vertical_spacing=0.1)

    data['rank'] = data.groupby('epoch')['activated_stake'].rank(method='dense', ascending=False)
    data['is_top30'] = data.groupby('epoch')['rank'].transform(lambda x: x <= x.quantile(0.3))
    top30_data = data[(data['version_digit'] == '2') & (data['is_top30'])]

    chart1_data = data.dropna(subset=['avg_priority_fees_per_block', 'avg_mev_per_block'])
    chart1_data = chart1_data[
        (chart1_data['avg_priority_fees_per_block'] > 0) & 
        (chart1_data['avg_mev_per_block'] > 0)
    ]
    grouped_data1 = chart1_data.groupby(['epoch', 'version_digit'])[['avg_priority_fees_per_block', 'avg_mev_per_block']].mean().reset_index()
    top30_chart1 = top30_data.dropna(subset=['avg_priority_fees_per_block', 'avg_mev_per_block'])
    top30_chart1 = top30_chart1[
        (top30_chart1['avg_priority_fees_per_block'] > 0) & 
        (top30_chart1['avg_mev_per_block'] > 0)
    ]
    top30_grouped1 = top30_chart1.groupby('epoch')[['avg_priority_fees_per_block', 'avg_mev_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data1[grouped_data1['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped1
        if not subset.empty:
            priority_values = (subset['avg_priority_fees_per_block'] / LAMPORTS_PER_SOL).round(7)
            mev_values = (subset['avg_mev_per_block'] / LAMPORTS_PER_SOL).round(7)
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=priority_values, 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                text=[f"P: {val:,.3f}" for val in priority_values],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=mev_values, 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                base=priority_values,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"M: {val:,.3f}" for val in mev_values],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

    chart2_data = data.dropna(subset=['avg_user_tx_per_block', 'avg_vote_tx_per_block'])
    chart2_data = chart2_data[
        (chart2_data['avg_user_tx_per_block'] > 0) & 
        (chart2_data['avg_vote_tx_per_block'] > 0)
    ]
    grouped_data2 = chart2_data.groupby(['epoch', 'version_digit'])[['avg_user_tx_per_block', 'avg_vote_tx_per_block']].mean().reset_index()
    top30_chart2 = top30_data.dropna(subset=['avg_user_tx_per_block', 'avg_vote_tx_per_block'])
    top30_chart2 = top30_chart2[
        (top30_chart2['avg_user_tx_per_block'] > 0) & 
        (top30_chart2['avg_vote_tx_per_block'] > 0)
    ]
    top30_grouped2 = top30_chart2.groupby('epoch')[['avg_user_tx_per_block', 'avg_vote_tx_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data2[grouped_data2['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped2
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_user_tx_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"U: {val:,.0f}" for val in subset['avg_user_tx_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_vote_tx_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                base=subset['avg_user_tx_per_block'],
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"V: {val:,.0f}" for val in subset['avg_vote_tx_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

    chart3_data = data.dropna(subset=['avg_cu_per_block'])
    chart3_data = chart3_data[chart3_data['avg_cu_per_block'] > 0]
    grouped_data3 = chart3_data.groupby(['epoch', 'version_digit'])[['avg_cu_per_block']].mean().reset_index()
    top30_chart3 = top30_data.dropna(subset=['avg_cu_per_block'])
    top30_chart3 = top30_chart3[top30_chart3['avg_cu_per_block'] > 0]
    top30_grouped3 = top30_chart3.groupby('epoch')[['avg_cu_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data3[grouped_data3['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped3
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_cu_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"{val:,.0f}" for val in subset['avg_cu_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=3, col=1)

    fig.update_layout(
        title=dict(
            text="Solana Validator Client Version - Average Per-Block Metrics",
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

    fig.update_xaxes(title_text="<b>Epoch</b>", row=3, col=1)
    fig.update_yaxes(title_text="Prio Fees + MEV", row=1, col=1)
    fig.update_yaxes(title_text="TX Count", row=2, col=1)
    fig.update_yaxes(title_text="Compute Units", row=3, col=1)

    epoch = end_epoch
    filename = f'epoch{epoch}_epoch_comparison_charts.html'
    save_chart_html(fig, "Solana Epoch Comparison Charts", filename)

    if epoch == max_epoch:
        filename = 'epoch_comparison_charts.html'
        save_chart_html(fig, "Solana Epoch Comparison Charts", filename)

def plot_epoch_metrics_with_stake_colors(epoch, max_epoch):
    file_path = get_output_path("last_ten_epoch_aggregate_data.json", 'json')
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        latest_epochs = data
        epochs = [epoch['epoch'] for epoch in latest_epochs]
        priority_fees = [epoch['total_validator_priority_fees'] or 0 for epoch in latest_epochs]
        signature_fees = [epoch['total_validator_signature_fees'] or 0 for epoch in latest_epochs]
        inflation_rewards = [
            (epoch['total_validator_inflation_rewards'] or 0) + (epoch['total_delegator_inflation_rewards'] or 0)
            for epoch in latest_epochs
        ]
        mev_earned = [epoch['total_mev_earned'] or 0 for epoch in latest_epochs]

        colors = ['#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2']

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=epochs, y=priority_fees, name="Total Priority Fees",
            marker_color=colors[0], 
            text=[f"{v:,.0f}" for v in priority_fees], 
            textposition='inside',
            textfont=dict(color="white")
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=signature_fees, name="Total Signature Fees",
            marker_color=colors[1], 
            base=priority_fees, 
            text=[f"{v:,.0f}" for v in signature_fees], 
            textposition='inside',
            textfont=dict(color="white")
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=mev_earned, name="Total MEV Earned",
            marker_color=colors[2], 
            base=[i+j for i,j in zip(priority_fees, signature_fees)],
            text=[f"{v:,.0f}" for v in mev_earned], 
            textposition='inside',
            textfont=dict(color="white")
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=inflation_rewards, name="Total Inflation Rewards",
            marker_color=colors[3], 
            base=[i+j+k for i,j,k in zip(priority_fees, signature_fees, mev_earned)],
            text=[f"{v:,.0f}" for v in inflation_rewards], 
            textposition='inside',
            textfont=dict(color="white")
        ))

        totals = [p + s + m + i for p, s, m, i in zip(priority_fees, signature_fees, mev_earned, inflation_rewards)]
        
        annotations = []
        for i, total in enumerate(totals):
            annotations.append(
                dict(
                    x=epochs[i], 
                    y=total, 
                    text=f"{total:,.0f}", 
                    showarrow=False,
                    yshift=10,
                    font=dict(size=12, color="black", family="Arial", weight='bold'),
                    bgcolor="white",
                    bordercolor="black",
                    borderwidth=1
                )
            )

        fig.update_layout(
            title=dict(
                text="Solana Epoch Metrics Overview",
                y=0.95, x=0.5, xanchor='center', font=dict(size=14, weight='bold', color="#333333")
            ),
            barmode='stack',
            xaxis_title="<b>Epoch</b>",
            yaxis_title="Value (SOL)",
            legend=dict(
                x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)
            ),
            margin=dict(t=150, b=250, l=50, r=50),
            height=900,
            width=1200,
            annotations=annotations
        )

        filename = f'epoch{epoch}_epoch_metrics_chart.html'
        save_chart_html(fig, "Solana Epoch Metrics Chart", filename)

        if epoch == max_epoch:
            filename = "epoch_metrics_chart.html"
            save_chart_html(fig, "Solana Epoch Metrics Chart", filename)

    except FileNotFoundError:
        logger.error(f"Error: The file {file_path} was not found.")
    except json.JSONDecodeError:
        logger.error("Error: Could not decode JSON from the file.")