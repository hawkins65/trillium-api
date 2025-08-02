#!/usr/bin/env python3
import argparse
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.graph_objects as go
import plotly.io as pio

# Database connection parameters
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "smilax"
DB_NAME = "sol_blocks"

def get_epoch_number():
    """Get epoch number from command line or user input after querying available epochs."""
    parser = argparse.ArgumentParser(description='Generate slot duration histogram for a given epoch')
    parser.add_argument('epoch', nargs='?', type=int, help='Epoch number to process')
    args = parser.parse_args()

    if args.epoch is not None:
        return args.epoch

    # Query available epochs
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            database=DB_NAME
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("SELECT DISTINCT epoch FROM slot_duration ORDER BY epoch")
        epochs = [row['epoch'] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        if not epochs:
            print("No epochs found in the slot_duration table.")
            sys.exit(1)

        print("Available epochs:", ", ".join(map(str, epochs)))
        while True:
            try:
                epoch = input("Enter epoch number: ").strip()
                epoch = int(epoch)
                if epoch in epochs:
                    return epoch
                print(f"Epoch {epoch} not found. Please choose from: {', '.join(map(str, epochs))}")
            except ValueError:
                print("Please enter a valid integer for the epoch number.")
    except Exception as e:
        print(f"Error querying available epochs: {e}")
        sys.exit(1)

def fetch_duration_data(epoch):
    """Query slot duration data for the given epoch, binned into 20 ms intervals."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            database=DB_NAME
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
        WITH stats AS (
            SELECT 
                AVG(duration) AS mean_duration,
                STDDEV_SAMP(duration) AS stddev_duration
            FROM slot_duration
            WHERE epoch = %s
                AND duration IS NOT NULL
                AND duration > 0
        ),
        bins AS (
            SELECT 
                FLOOR(duration / 20000000.0) * 20 AS bin_start_ms,
                COUNT(*) AS slot_count
            FROM slot_duration
            WHERE epoch = %s
                AND duration IS NOT NULL
                AND duration > 0
            GROUP BY FLOOR(duration / 20000000.0)
        ),
        thresholds AS (
            SELECT 
                ROUND(mean_duration / 1000000.0, 2) AS mean_duration_ms,
                ROUND((mean_duration - 2 * stddev_duration) / 1000000.0, 2) AS low_outlier_threshold_ms,
                ROUND((mean_duration + 2 * stddev_duration) / 1000000.0, 2) AS high_outlier_threshold_ms
            FROM stats
        )
        SELECT 
            bins.bin_start_ms,
            bins.slot_count,
            thresholds.mean_duration_ms,
            thresholds.low_outlier_threshold_ms,
            thresholds.high_outlier_threshold_ms
        FROM bins
        CROSS JOIN thresholds
        ORDER BY bins.bin_start_ms
        """
        cursor.execute(query, (epoch, epoch))
        data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        if not data:
            print(f"No data found for epoch {epoch}.")
            sys.exit(1)
        
        return data
    except Exception as e:
        print(f"Error querying slot duration data: {e}")
        sys.exit(1)

def plot_histogram(data, epoch):
    """Generate and save an HTML histogram of slot durations with enhanced visuals."""
    # Extract data and convert to float, filter bins > 1000 ms
    bin_starts = [float(row['bin_start_ms']) for row in data if float(row['bin_start_ms']) <= 1000]
    slot_counts = [float(row['slot_count']) for row in data if float(row['bin_start_ms']) <= 1000]
    mean_duration = float(data[0]['mean_duration_ms'])
    low_threshold = float(data[0]['low_outlier_threshold_ms'])
    high_threshold = float(data[0]['high_outlier_threshold_ms'])
    
    # Calculate outlier counts and percentages based on filtered data
    total_slots = sum(slot_counts)
    low_outlier_count = sum(count for bs, count in zip(bin_starts, slot_counts) if bs < low_threshold)
    high_outlier_count = sum(count for bs, count in zip(bin_starts, slot_counts) if bs >= high_threshold)
    low_outlier_percent = (low_outlier_count / total_slots * 100) if total_slots > 0 else 0
    high_outlier_percent = (high_outlier_count / total_slots * 100) if total_slots > 0 else 0
    
    # Create figure with adjusted width
    fig = go.Figure()
    
    # Add histogram bars with color coding
    fig.add_trace(go.Bar(
        x=bin_starts,
        y=slot_counts,
        width=18,
        marker_color=[ '#FF6666' if bs < low_threshold else '#66B2FF' if bs >= high_threshold else '#4CAF50' for bs in bin_starts],
        marker_line_color='black',
        marker_line_width=0.5,
        name='Slot Durations',
        showlegend=False
    ))
    
    # Add mean line
    fig.add_vline(
        x=mean_duration, 
        line_dash="dash", 
        line_color="black", 
        line_width=2
    )
    
    # Add threshold lines
    fig.add_vline(
        x=low_threshold, 
        line_dash="dash", 
        line_color="red", 
        line_width=2
    )
    
    fig.add_vline(
        x=high_threshold, 
        line_dash="dash", 
        line_color="blue", 
        line_width=2
    )
    
    # Get max y value for positioning annotations
    max_y = max(slot_counts) if slot_counts else 1000
    
    # Customize layout
    fig.update_layout(
        title={
            'text': f"Slot Duration Distribution for Epoch {epoch} (Solana Network)",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=20, color='black', family='sans-serif', weight='bold')
        },
        xaxis_title="Slot Duration (ms)",
        yaxis_title="Number of Slots",
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=50,
            tickangle=45,
            title_font=dict(size=12, weight='bold'),
            tickfont=dict(size=10),
            range=[0, 1000],
            gridcolor='lightgray',
            griddash='dash',
            gridwidth=0.5
        ),
        yaxis=dict(
            title_font=dict(size=12, weight='bold'),
            tickfont=dict(size=10),
            gridcolor='lightgray',
            griddash='dash',
            gridwidth=0.5
        ),
        margin=dict(t=80, b=100, l=50, r=50),  # Changed from t=150 to t=100 to reduce gap
        height=850,
        width=800,
        showlegend=False,
        plot_bgcolor='white',
        paper_bgcolor='white',
        annotations=[
            # Mean line annotation
            dict(
                x=mean_duration,
                y=max_y * 0.9,
                text=f"Mean: {mean_duration} ms",
                showarrow=True,
                arrowhead=2,
                arrowcolor="black",
                ax=20,
                ay=-30,
                font=dict(size=12, color="black"),
                bgcolor="white",
                bordercolor="black",
                borderwidth=1
            ),
            # Low threshold annotation
            dict(
                x=low_threshold,
                y=max_y * 0.8,
                text=f"Super Fast<br>(< {low_threshold} ms)<br>{low_outlier_percent:.2f}%",
                showarrow=True,
                arrowhead=2,
                arrowcolor="red",
                ax=30,
                ay=-30,
                font=dict(size=11, color="red"),
                bgcolor="white",
                bordercolor="red",
                borderwidth=1
            ),
            # High threshold annotation
            dict(
                x=high_threshold,
                y=max_y * 0.7,
                text=f"Slower than Expected<br>(> {high_threshold} ms)<br>{high_outlier_percent:.2f}%",
                showarrow=True,
                arrowhead=2,
                arrowcolor="blue",
                ax=-40,
                ay=-30,
                font=dict(size=11, color="blue"),
                bgcolor="white",
                bordercolor="blue",
                borderwidth=1
            ),
            # Note about values beyond 1000 ms
            dict(
                text="Values extend beyond 1000 ms",
                xref="paper", yref="paper",
                x=0.98, y=0.05,
                showarrow=False,
                font=dict(size=10, color="gray"),
                align="right"
            )
        ],
        images=[
            dict(
                source="https://trillium.so/images/fueled-by-trillium.png",
                xref="paper", yref="paper",
                x=0.99, y=0.99, 
                sizex=0.2, sizey=0.066,
                xanchor="right", yanchor="top"
            )
        ]
    )
    
    # Save as HTML with favicon and custom title
    output_file = f'slot_duration_histogram_epoch{epoch}.html'

    # 1) Render the full HTML to a string (with clickable logo via post_script)
    html = pio.to_html(
        fig,
        include_plotlyjs='cdn',
        config={'responsive': True},
        full_html=True,
        post_script=f"""
        // wrap the first SVG <image> in a clickable <a>
        var chartDiv = document.getElementsByClassName('plotly-graph-div')[0];
        var img = chartDiv.querySelector('image');
        if (img) {{
            var xmlns = 'http://www.w3.org/2000/svg';
            var xlink = 'http://www.w3.org/1999/xlink';
            // create the anchor
            var link = document.createElementNS(xmlns, 'a');
            link.setAttributeNS(xlink, 'xlink:href', 'https://trillium.so');
            link.setAttribute('target', '_blank');
            // move the image inside the link
            img.parentNode.insertBefore(link, img);
            link.appendChild(img);
        }}
        """
    )

    # 2) Inject the Trillium favicon into the <head>
    favicon_link = '<link rel="icon" href="https://trillium.so/favicon.ico" />'
    html = html.replace(
        '<head>',
        '<head>\n    ' + favicon_link
    )

    # 3) Write the modified HTML back out
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"Histogram saved to {output_file}")


def main():
    """Main function to query data and plot histogram."""
    epoch = get_epoch_number()
    data = fetch_duration_data(epoch)
    plot_histogram(data, epoch)

if __name__ == "__main__":
    main()
