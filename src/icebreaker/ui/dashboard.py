from services.metrics_service import MetricsService
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import altair as alt
import plotly.express as px
from plotly.subplots import make_subplots
import asyncio
from services.assistant import Assistant
from services.recommendation_service import RecommendationService

def apply_sidebar_style():
    """Apply custom styling to the sidebar with ice theme."""
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #EFF8FF 0%, #E6F4FF 100%);
            border-right: 1px solid #A5D8FF;
        }
        .stButton button {
            width: 100%;
            text-align: center !important;
            padding: 0.5rem 1rem !important;
            background: linear-gradient(135deg, #EFF8FF 0%, #E6F4FF 100%) !important;
            border: 1px solid #A5D8FF !important;
            color: #2C5282 !important;
            font-size: 0.9rem !important;
            border-radius: 8px !important;
            transition: all 0.2s;
        }
        .stButton button:hover {
            background: linear-gradient(135deg, #E6F4FF 0%, #D9EDFF 100%) !important;
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(165, 216, 255, 0.4);
        }
        </style>
        """,
        unsafe_allow_html=True
    )

def render_sidebar(tables_df):
    """Render the sidebar with table selection functionality."""
    st.sidebar.header("Available Tables")
    search_query = st.sidebar.text_input("Search for a table")
    
    # Filter tables based on search
    if search_query:
        filtered_tables_df = tables_df[tables_df['source_table'].str.contains(search_query, case=False, na=False)]
    else:
        filtered_tables_df = tables_df

    # Display table names and handle selection
    for _, row in filtered_tables_df.iterrows():
        table_name = row['source_table']
        if st.sidebar.button(
            table_name,
            key=f"button_{table_name}",
            help="Click to view table details",
            type="secondary"
        ):
            st.session_state['selected_table'] = table_name

    return st.session_state.get('selected_table', None)

def get_metric_card_html(label, value):
    """Generate HTML for a metric card with an enhanced ice-cube theme styling."""
    # Special handling for timestamp to keep it on one line
    if "Last Updated Timestamp" in label:
        value_style = """
            font-size: 24px; 
            background: linear-gradient(45deg, #2B6CB0, #4299E1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-align: center;
            font-weight: 600;
            white-space: nowrap;
        """
    else:
        value_style = """
            font-size: 36px; 
            background: linear-gradient(45deg, #2B6CB0, #4299E1);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-align: center;
            font-weight: 600;
        """

    return f"""
    <div style="
        border: 2px solid rgba(165, 216, 255, 0.5); 
        border-radius: 20px; 
        padding: 25px; 
        background: linear-gradient(135deg, rgba(239, 248, 255, 0.9) 0%, rgba(230, 244, 255, 0.8) 100%);
        box-shadow: 0 8px 20px rgba(165, 216, 255, 0.2),
                   inset 0 -2px 6px rgba(255, 255, 255, 0.8),
                   inset 0 2px 6px rgba(165, 216, 255, 0.4); 
        height: 160px; 
        display: flex; 
        flex-direction: column; 
        margin: 15px auto;
        width: 92%;
        backdrop-filter: blur(8px);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    ">
        <div class="ice-shine"></div>
        <div style="
            margin-bottom: 20px;
            min-height: 40px;
            display: flex;
            align-items: center;
            justify-content: center;
        ">
            <strong style="
                font-size: 16px; 
                color: #2C5282; 
                line-height: 1.3;
                text-align: center;
                letter-spacing: 0.3px;
                text-shadow: 0 1px 2px rgba(255, 255, 255, 0.8);
            ">{label}</strong>
        </div>
        <div style="
            display: flex; 
            align-items: center; 
            justify-content: center; 
            flex-grow: 1;
            position: relative;
        ">
            <span style="{value_style}">{value}</span>
        </div>
    </div>
    <style>
    .ice-shine {{
        position: absolute;
        top: 0;
        left: -100%;
        width: 50%;
        height: 100%;
        background: linear-gradient(
            90deg,
            transparent,
            rgba(255, 255, 255, 0.2),
            transparent
        );
        animation: shine 3s infinite;
        pointer-events: none;
    }}
    @keyframes shine {{
        0% {{ left: -100%; }}
        50% {{ left: 200%; }}
        100% {{ left: -100%; }}
    }}
    </style>
    """

def get_table_detail_card_html(label, value):
    """Generate HTML for a table detail metric card with adjusted sizing."""
    value_font_size = "36px"  # default size
    if len(str(value)) > 15:
        value_font_size = "24px"
    if len(str(value)) > 25:
        value_font_size = "20px"

    return f"""
    <div style="
        border: 2px solid rgba(165, 216, 255, 0.5); 
        border-radius: 20px; 
        padding: 20px 15px; 
        background: linear-gradient(135deg, rgba(239, 248, 255, 0.9) 0%, rgba(230, 244, 255, 0.8) 100%);
        box-shadow: 0 8px 20px rgba(165, 216, 255, 0.2),
                   inset 0 -2px 6px rgba(255, 255, 255, 0.8),
                   inset 0 2px 6px rgba(165, 216, 255, 0.4); 
        height: 140px; 
        display: flex; 
        flex-direction: column; 
        margin: 10px auto;
        width: 95%;
        backdrop-filter: blur(8px);
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    ">
        <div class="ice-shine"></div>
        <div style="
            margin-bottom: 15px;
            min-height: 35px;
            display: flex;
            align-items: center;
            justify-content: center;
        ">
            <strong style="
                font-size: 16px; 
                color: #2C5282; 
                line-height: 1.2;
                text-align: center;
                letter-spacing: 0.3px;
                text-shadow: 0 1px 2px rgba(255, 255, 255, 0.8);
            ">{label}</strong>
        </div>
        <div style="
            display: flex; 
            align-items: center; 
            justify-content: center; 
            flex-grow: 1;
            position: relative;
        ">
            <span style="
                font-size: {value_font_size}; 
                background: linear-gradient(45deg, #2B6CB0, #4299E1);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-align: center;
                font-weight: 600;
                padding: 0 10px;
                word-break: break-word;
            ">{value}</span>
        </div>
    </div>
    <style>
    .ice-shine {{
        position: absolute;
        top: 0;
        left: -100%;
        width: 50%;
        height: 100%;
        background: linear-gradient(
            90deg,
            transparent,
            rgba(255, 255, 255, 0.2),
            transparent
        );
        animation: shine 3s infinite;
        pointer-events: none;
    }}
    @keyframes shine {{
        0% {{ left: -100%; }}
        50% {{ left: 200%; }}
        100% {{ left: -100%; }}
    }}
    </style>
    """

def display_metrics_grid(metrics_data):
    """Display metrics in a grid layout with two rows."""
    # First row - 3 columns
    cols = st.columns(3)
    for i in range(3):
        label, value = metrics_data[i]
        with cols[i]:
            st.markdown(get_metric_card_html(label, value), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Second row - 3 columns
    cols = st.columns(3)
    for i in range(3, 6):
        label, value = metrics_data[i]
        with cols[i - 3]:
            st.markdown(get_metric_card_html(label, value), unsafe_allow_html=True)

def display_overall_metrics(namespace_statistics):
    """Display the overall metrics section with enhanced ice theme."""
    st.markdown("""
        <h1 style='
            color: #2C5282;
            text-align: center;
            margin-top: 3rem;
            margin-bottom: 4rem;
            padding-top: 2rem;
            font-size: 3.2rem;
            background: linear-gradient(45deg, #2B6CB0, #63B3ED);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            letter-spacing: 0.5px;
        '>Tables Insights Hub</h1>
        <h2 style='
            color: #4A5568;
            text-align: center;
            margin-bottom: 3rem;
            font-size: 1.8rem;
            opacity: 0.9;
            font-weight: 500;
        '>Overall Metrics for All Tables</h2>
    """, unsafe_allow_html=True)
    
    overall_metrics = [
        ("Total Number of Tables", namespace_statistics['total_tables']),
        ("Total Number of Files in Namespace", namespace_statistics['total_files']),
        ("Total File Size in Namespace (MB)", f"{round(namespace_statistics['total_file_size'])} MB"),
        ("Last Updated Timestamp", namespace_statistics['last_updated'].strftime('%Y-%m-%d %H:%M:%S') if namespace_statistics['last_updated'] else "N/A"),
        ("Total Number of Active Files in Namespace", namespace_statistics['total_active_files']),
        ("Total Active File Size in Namespace (MB)", f"{round (namespace_statistics['total_active_file_size'])} MB")
    ]
    
    display_metrics_grid(overall_metrics)
    st.markdown("<br><br>", unsafe_allow_html=True)

def display_table_metrics(table_metrics_df):
    """Display table metrics visualization and interactive table."""
    # Use full width container with minimal padding
    st.markdown("""
        <style>
        .block-container {
            padding: 0rem 1rem;
            max-width: 100%;
        }
        .element-container {
            width: 100%;
        }
        .stPlotlyChart {
            width: 100% !important;
        }
        </style>
    """, unsafe_allow_html=True)

    # Sort dataframe by total size
    df_sorted = table_metrics_df.sort_values('total_size_mb', ascending=False)

    # Add filtering options
    st.markdown("""
        <h3 style='
            color: #2C5282;
            text-align: center;
            margin: 2rem 0;
            font-size: 1.4rem;
            background: linear-gradient(45deg, #2B6CB0, #63B3ED);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            letter-spacing: 0.5px;
            font-weight: 700;
        '>Table Metrics</h3>
    """, unsafe_allow_html=True)
    
    # Create three columns with the middle one being for the input
    col1, col2, col3 = st.columns([1, 1, 1])
    
    # Place the number input in the middle column
    with col1:
        num_tables = st.number_input(
            "Number of tables to display",
            min_value=1,
            max_value=len(df_sorted),
            value=20
        )

    # Apply filter
    filtered_df = df_sorted.head(num_tables)

    # Display chart in full width
    st.markdown("""
        <h3 style='
            color: #4A5568;
            text-align: center;
            margin: 2rem 0;
            font-size: 1.4rem;
            opacity: 0.9;
            font-weight: 500;
        '>Table Size Distribution</h3>
    """, unsafe_allow_html=True)
    
    # Create the figure
    fig = go.Figure()
    
    # Add bar chart for total size with darker blue
    fig.add_trace(go.Bar(
        name='Total Size',
        x=filtered_df['source_table'],
        y=filtered_df['total_size_mb'],
        marker_color='rgba(30, 136, 229, 0.7)',  # Darker and more opaque blue
        hovertemplate='Total Size: %{y:.1f} MB<extra></extra>'
    ))
    
    # Add line chart for active size with a different color and style
    fig.add_trace(go.Scatter(
        name='Active Size',
        x=filtered_df['source_table'],
        y=filtered_df['active_size_mb'],
        mode='lines+markers',
        line=dict(
            color='rgb(211, 47, 47)',  # Changed to a contrasting red color
            width=2.5
        ),
        marker=dict(
            size=8,
            symbol='circle',
            color='rgb(211, 47, 47)',
            line=dict(
                color='rgb(211, 47, 47)',
                width=2
            )
        ),
        hovertemplate='Active Size: %{y:.1f} MB<extra></extra>'
    ))

    fig.update_layout(
        xaxis_title='Table Name',
        yaxis_title='Size (MB)',
        height=500,
        plot_bgcolor='rgba(255,255,255,0.9)',
        paper_bgcolor='rgba(255,255,255,0)',
        margin=dict(
            l=50,
            r=50,
            b=100,
            t=30,
            pad=4
        ),
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="right",
            x=0.99,
            bgcolor='rgba(255,255,255,0.8)',
            bordercolor='rgba(211,211,211,0.5)',
            borderwidth=1
        ),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211,211,211,0.5)',
            tickfont=dict(size=10),
            tickangle=-45
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(211,211,211,0.5)',
            tickfont=dict(size=10),
            title_font=dict(size=12)
        ),
        hovermode='x unified'
    )

    st.plotly_chart(fig, use_container_width=True)

    # Add some spacing
    st.markdown("<br>", unsafe_allow_html=True)

    # Display table in full width
    st.markdown("""
        <h3 style='
            color: #4A5568;
            text-align: center;
            margin: 2rem 0;
            font-size: 1.4rem;
            opacity: 0.9;
            font-weight: 500;
        '>Detailed Metrics by Table</h3>
    """, unsafe_allow_html=True)

    display_df = df_sorted.copy()
    display_df['active_size_mb'] = display_df['active_size_mb'].round(2)
    display_df['total_size_mb'] = display_df['total_size_mb'].round(2)
    
    # Calculate size difference percentage
    display_df['size_diff_pct'] = ((display_df['total_size_mb'] - display_df['active_size_mb']) / display_df['total_size_mb'] * 100).round(2)
    
    display_df.columns = ['Table Name', 'Active Files', 'Total Files', 
                         'Active Size (MB)', 'Total Size (MB)',
                         'Total Records', 'Active Records', 'Size Diff %']

    # Define the styling function
    def highlight_size_differences(row):
        styles = [''] * len(row)
        if row['Size Diff %'] > 50:  # You can adjust this threshold
            styles[3:5] = [
                'background-color: rgba(255, 0, 0, 0.2)',  # Light red for Active Size
                'background-color: rgba(255, 0, 0, 0.2)'   # Light red for Total Size
            ]
        return styles

    # Apply styling and display
    styled_df = display_df.style.apply(highlight_size_differences, axis=1)
    
    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=400
    )

def display_table_detail_grid(metrics_data):
    """Display metrics in a grid layout with two rows using the table detail card style."""
    cols = st.columns(3)
    for i in range(3):
        label, value = metrics_data[i]
        with cols[i]:
            st.markdown(get_table_detail_card_html(label, value), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    cols = st.columns(3)
    for i in range(3, 6):
        label, value = metrics_data[i]
        with cols[i - 3]:
            st.markdown(get_table_detail_card_html(label, value), unsafe_allow_html=True)

def get_selected_table_metric_card_html(label, value, color):
    """Generate HTML for a metric card with styling similar to Table Insights Hub."""
    return f"""
        <div style="
            border: 2px solid rgba(165, 216, 255, 0.5); 
            border-radius: 20px; 
            padding: 25px; 
            background: linear-gradient(135deg, rgba(239, 248, 255, 0.9) 0%, rgba(230, 244, 255, 0.8) 100%);
            box-shadow: 0 8px 20px rgba(165, 216, 255, 0.2),
                       inset 0 -2px 6px rgba(255, 255, 255, 0.8),
                       inset 0 2px 6px rgba(165, 216, 255, 0.4); 
            height: 180px; 
            display: flex; 
            flex-direction: column; 
            margin: 20px auto;
            width: 100%;
            backdrop-filter: blur(8px);
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        ">
            <div class="ice-shine"></div>
            <div style="
                margin-bottom: 20px;
                min-height: 40px;
                display: flex;
                align-items: center;
                justify-content: center;
            ">
                <strong style="
                    font-size: 14px; 
                    color: #2C5282; 
                    line-height: 1.3;
                    text-align: center;
                    letter-spacing: 0.3px;
                    text-shadow: 0 1px 2px rgba(255, 255, 255, 0.8);
                ">{label}</strong>
            </div>
            <div style="
                display: flex; 
                align-items: center; 
                justify-content: center; 
                flex-grow: 1;
                position: relative;
            ">
                <span style="
                    font-size: 26px; 
                    color: {color};
                    text-align: center;
                    font-weight: 600;
                ">{value}</span>
            </div>
        </div>
        <style>
        .ice-shine {{
            position: absolute;
            top: 0;
            left: -100%;
            width: 50%;
            height: 100%;
            background: linear-gradient(
                90deg,
                transparent,
                rgba(255, 255, 255, 0.2),
                transparent
            );
            animation: shine 3s infinite;
            pointer-events: none;
        }}
        @keyframes shine {{
            0% {{ left: -100%; }}
            50% {{ left: 200%; }}
            100% {{ left: -100%; }}
        }}
        </style>
    """

def display_selected_table_metrics(metrics_data):
    """Display metrics for a selected table with 3 cards per row."""
    st.markdown("<div style='margin-top: 40px; margin-bottom: 20px;'></div>", unsafe_allow_html=True)
    
    # Add custom CSS to increase spacing between columns and expand container width
    st.markdown("""
        <style>
        .row-widget.stHorizontal {
            gap: 25px !important;
        }
        .block-container {
            max-width: 95% !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Display metrics in rows of 3 for larger cards
    for i in range(0, len(metrics_data), 3):
        cols = st.columns(3, gap="large")
        for col_idx, metric_idx in enumerate(range(i, min(i+3, len(metrics_data)))):
            if metric_idx < len(metrics_data):  # Make sure we don't go out of bounds
                metric = metrics_data[metric_idx]
                with cols[col_idx]:
                    st.markdown(
                        get_selected_table_metric_card_html(
                            metric["label"], 
                            metric["value"], 
                            metric["color"]
                        ), 
                        unsafe_allow_html=True
                    )
        
        # Add vertical spacing between rows
        st.markdown("<div style='margin-top: 15px;'></div>", unsafe_allow_html=True)

def display_section_header(title):
    """Display a section header with a divider line."""
    st.markdown(f"""
        <div style='
            margin: 40px 0;
            height: 2px;
            background: linear-gradient(90deg, 
                rgba(165, 216, 255, 0.1),
                rgba(165, 216, 255, 0.8),
                rgba(165, 216, 255, 0.1));
            border-radius: 2px;
        '></div>
        <h3 style='
            color: #2C5282;
            text-align: center;
            margin: 2rem 0;
            font-size: 1.8rem;
            background: linear-gradient(45deg, #2B6CB0, #63B3ED);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            letter-spacing: 0.5px;
        '>{title}</h3>
    """, unsafe_allow_html=True)

def get_filtered_metrics_data(metrics_service, selected_table):
    """Get and filter metrics data based on selected time range."""
    table_growth_data = metrics_service.get_table_growth_analysis(selected_table)
    table_growth_data['date'] = pd.to_datetime(table_growth_data['date'])
    
    st.markdown("<div style='margin-bottom: 20px;'>", unsafe_allow_html=True)
    time_ranges = {
        "1 Day": 1,
        "5 Days": 5,
        "1 Month": 30,
        "6 Months": 180,
        "1 Year": 365,
        "All Time": None
    }
    
    selected_range = st.radio(
        "Select Time Range",
        options=list(time_ranges.keys()),
        horizontal=True
    )
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Filter data based on selected time range
    if time_ranges[selected_range] is not None:
        end_date = table_growth_data['date'].max()
        start_date = end_date - pd.Timedelta(days=time_ranges[selected_range])
        daily_metrics_df = table_growth_data[table_growth_data['date'] >= start_date]
    else:
        daily_metrics_df = table_growth_data
    
    return daily_metrics_df

def create_growth_charts(daily_metrics_df):
    """Create charts for growth analysis."""
    base_tooltip = [
        alt.Tooltip('date:T', title='Date'),
        alt.Tooltip('value:Q', title='Value'),
        alt.Tooltip('metric:N', title='Metric')
    ]
    
    records_chart = alt.Chart(daily_metrics_df).mark_line().encode(
        x='date:T',
        y=alt.Y('value:Q', title='Records'),
        color=alt.Color('metric:N', 
                      scale=alt.Scale(domain=['Total Records', 'Records Added'],
                                    range=['#2ecc71', '#27ae60']),
                      legend=alt.Legend(title='Records Metrics')),
        strokeDash=alt.condition(
            alt.datum.metric == 'Records Added',
            alt.value([5,5]),
            alt.value([0])
        ),
        tooltip=base_tooltip + [
            alt.Tooltip('total_records:Q', title='Total Records'),
            alt.Tooltip('records_added:Q', title='Records Added')
        ]
    ).transform_fold(
        ['total_records', 'records_added'],
        as_=['metric', 'value']
    ).transform_calculate(
        metric=alt.expr.if_(
            alt.datum.metric == 'total_records',
            'Total Records',
            'Records Added'
        )
    ).properties(
        width=300,
        height=200,
        title='Records Growth'
    )

    files_chart = alt.Chart(daily_metrics_df).mark_line().encode(
        x='date:T',
        y=alt.Y('value:Q', title='Files'),
        color=alt.Color('metric:N',
                      scale=alt.Scale(domain=['Total Files', 'Files Added'],
                                    range=['#3498db', '#2980b9']),
                      legend=alt.Legend(title='Files Metrics')),
        strokeDash=alt.condition(
            alt.datum.metric == 'Files Added',
            alt.value([5,5]),
            alt.value([0])
        ),
        tooltip=base_tooltip + [
            alt.Tooltip('total_data_files:Q', title='Total Files'),
            alt.Tooltip('files_added:Q', title='Files Added')
        ]
    ).transform_fold(
        ['total_data_files', 'files_added'],
        as_=['metric', 'value']
    ).transform_calculate(
        metric=alt.expr.if_(
            alt.datum.metric == 'total_data_files',
            'Total Files',
            'Files Added'
        )
    ).properties(
        width=300,
        height=200,
        title='Files Growth'
    )

    size_chart = alt.Chart(daily_metrics_df).mark_line().encode(
        x='date:T',
        y=alt.Y('value:Q', title='Size (MB)'),
        color=alt.Color('metric:N',
                      scale=alt.Scale(domain=['Total Size', 'Size Added'],
                                    range=['#9b59b6', '#8e44ad']),
                      legend=alt.Legend(title='Size Metrics')),
        strokeDash=alt.condition(
            alt.datum.metric == 'Size Added',
            alt.value([5,5]),
            alt.value([0])
        ),
        tooltip=base_tooltip + [
            alt.Tooltip('total_file_size:Q', title='Total Size (MB)', format='.2f'),
            alt.Tooltip('size_added:Q', title='Size Added (MB)', format='.2f')
        ]
    ).transform_fold(
        ['total_file_size', 'size_added'],
        as_=['metric', 'value']
    ).transform_calculate(
        metric=alt.expr.if_(
            alt.datum.metric == 'total_file_size',
            'Total Size',
            'Size Added'
        )
    ).properties(
        width=300,
        height=200,
        title='Size Growth'
    )
    
    return records_chart, files_chart, size_chart

def display_growth_metrics(daily_metrics_df):
    """Display growth metrics in a grid layout."""
    col_stats1, col_stats2, col_stats3 = st.columns(3)
    with col_stats1:
        st.metric(
            "Total Records Growth",
            f"{daily_metrics_df['total_records'].iloc[0]:,.0f}",
            f"{daily_metrics_df['total_records'].iloc[0] - (daily_metrics_df['total_records'].iloc[-1] if len(daily_metrics_df) > 1 else 0):,.0f} vs prev day" 
        )
    with col_stats2:
        st.metric(
            "Total Files Added", 
            f"{daily_metrics_df['total_data_files'].iloc[0]:,.0f}",
            f"{daily_metrics_df['total_data_files'].iloc[0] - (daily_metrics_df['total_data_files'].iloc[-1] if len(daily_metrics_df) > 1 else 0):,.0f} vs prev day" 
        )
    with col_stats3:
        st.metric(
            "Total Size Added",
            f"{daily_metrics_df['total_file_size'].iloc[0]:,.1f} MB",
            f"{daily_metrics_df['total_file_size'].iloc[0] - (daily_metrics_df['total_file_size'].iloc[-1] if len(daily_metrics_df) > 1 else 0):,.1f} MB vs prev day" 
        )
    st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)

def display_growth_charts(records_chart, files_chart, size_chart):
    """Display growth charts in a grid layout."""
    col1, col2 = st.columns(2)
    with col1:
        st.altair_chart(records_chart, use_container_width=True)
        st.markdown("<h5 style='text-align: center; color: #2c3e50; font-size: 14px;'>Records Growth (Total vs Daily Added)</h5>", unsafe_allow_html=True)
    with col2:
        st.altair_chart(files_chart, use_container_width=True)
        st.markdown("<h5 style='text-align: center; color: #2c3e50; font-size: 14px;'>Files Growth (Total vs Daily Added)</h5>", unsafe_allow_html=True)

    st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)
        
    col3 = st.columns([1, 2, 1])[1]
    with col3:
        st.altair_chart(size_chart, use_container_width=True)
        st.markdown("<h5 style='text-align: center; color: #2c3e50; font-size: 14px;'>Size Growth (Total vs Daily Added)</h5>", unsafe_allow_html=True)

    st.markdown("<div style='margin-bottom: 40px;'></div>", unsafe_allow_html=True)

def display_partition_analysis(metrics_service, selected_table):
    """Display partition analysis with table and visualizations."""
    # Get partition details
    partition_df = metrics_service.get_partition_details(selected_table)
    
    # Format the last_updated_at column
    if 'last_updated_at' in partition_df.columns:
        partition_df['last_updated_at'] = pd.to_datetime(partition_df['last_updated_at']).dt.strftime('%Y-%m-%d %H:%M')
    
    # Display partition details table
    st.subheader("Partition Details")
    
    # Select and reorder columns for display
    display_columns = ['partition_key', 'spec_id', 'last_updated_at', 'avg_file_size_mb', 
                       'active_record_count', 'all_total_records', 'active_file_count', 'total_files']
    
    # Format the avg_file_size_mb column
    partition_df['avg_file_size_mb'] = partition_df['avg_file_size_mb'].round(2)
    
    # Display the table with selected columns
    st.dataframe(partition_df[display_columns].set_index('partition_key'), use_container_width=True)
    
    # Create visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Pie chart for record distribution across partitions
        # Calculate percentage of records for each partition
        partition_df['record_percentage'] = (partition_df['active_record_count'] / 
                                            partition_df['active_record_count'].sum() * 100)
        
        fig_pie = px.pie(
            partition_df, 
            values='active_record_count',
            names='partition_key',
            title='Record Distribution Across Partitions',
            color_discrete_sequence=px.colors.qualitative.Bold,
            hole=0.4
        )
        
        fig_pie.update_traces(
            textposition='inside', 
            textinfo='percent+label',
            marker=dict(line=dict(color='#FFFFFF', width=2))
        )
        
        fig_pie.update_layout(
            legend_title_text='Partition Key',
            legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5)
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Bar chart for size and file metrics
        # Create figure with secondary y-axis
        fig_bar = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add bars for total size
        fig_bar.add_trace(
            go.Bar(
                x=partition_df['partition_key'],
                y=partition_df['total_size_mb'],
                name="Total Size (MB)",
                marker_color='#3498db',
                opacity=0.8
            ),
            secondary_y=False,
        )
        
        # Add bars for active size
        fig_bar.add_trace(
            go.Bar(
                x=partition_df['partition_key'],
                y=partition_df['active_total_data_file_size_mb'],
                name="Active Size (MB)",
                marker_color='#2ecc71',
                opacity=0.8
            ),
            secondary_y=False,
        )
        
        # Add line for file count
        fig_bar.add_trace(
            go.Scatter(
                x=partition_df['partition_key'],
                y=partition_df['total_files'],
                name="Total Files",
                mode='lines+markers',
                marker=dict(size=8, color='#e74c3c'),
                line=dict(width=3, dash='dot')
            ),
            secondary_y=True,
        )
        
        # Set titles
        fig_bar.update_layout(
            title_text="Partition Size and File Metrics",
            barmode='group',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5)
        )
        
        # Set y-axes titles
        fig_bar.update_yaxes(title_text="Size (MB)", secondary_y=False)
        fig_bar.update_yaxes(title_text="File Count", secondary_y=True)
        
        st.plotly_chart(fig_bar, use_container_width=True)

def display_file_size_analysis(metrics_service, selected_table):
    """Display file size distribution analysis across partitions."""
    # Get file size data
    file_size_df = metrics_service.get_partition_file_size(selected_table)
    
    if file_size_df.empty:
        st.warning("No file size data available for this table.")
        return
    
    # Calculate average file size for each partition and overall
    partition_avg = file_size_df.groupby('partition_key')['file_size_mb'].mean().reset_index()
    partition_avg = partition_avg.rename(columns={'file_size_mb': 'avg_file_size_mb'})
    
    overall_avg = file_size_df['file_size_mb'].mean()
    
    # Merge average back to the main dataframe
    file_size_df = file_size_df.merge(partition_avg, on='partition_key')
    
    # Calculate deviation from partition average and overall average
    file_size_df['deviation_from_partition_avg'] = file_size_df['file_size_mb'] - file_size_df['avg_file_size_mb']
    file_size_df['deviation_from_overall_avg'] = file_size_df['file_size_mb'] - overall_avg
    
    # Add space before the header
    st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)
    
    # Display overall deviation chart
    st.subheader("File Size Deviation from Average")
    
    # Create partition selector and compact button in the same row
    col_select, col_button1, col_button2, col_spacer = st.columns([3, 1, 1, 1])
    
    # Get list of partitions
    partitions = file_size_df['partition_key'].unique().tolist()
    
    # Create dropdown for partition selection
    with col_select:
        selected_partition = st.selectbox(
            "Select Partition for Detailed View",
            partitions,
            index=0
        )
    
    # Create compact button
    with col_button1:
        st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)  # Align with selectbox
        compact_clicked = st.button("Compact", key="compact_button")
        
        if compact_clicked:
            # Call the compaction method
            try:
                # Replace this line with your actual method call
                metrics_service.run_compaction_job(selected_table)
                st.success(f"Compaction job started for table '{selected_table}'")
            except Exception as e:
                st.error(f"Error starting compaction: {str(e)}")
    
    # Create vacuum button
    with col_button2:
        st.markdown("<div style='margin-top: 24px;'></div>", unsafe_allow_html=True)  # Align with selectbox
        vacuum_clicked = st.button("Vaccum", key="vacuum_button")
        
        if vacuum_clicked:
            # Call the vacuum method
            try:
                # Replace this line with your actual method call
                metrics_service.run_vacuum(selected_table)
                st.success(f"Vacuum job started for partition '{selected_partition}' in table '{selected_table}'")
            except Exception as e:
                st.error(f"Error starting vacuum: {str(e)}")
    
    # Filter data for selected partition
    partition_files = file_size_df[file_size_df['partition_key'] == selected_partition]
    partition_avg_size = partition_files['avg_file_size_mb'].iloc[0]
    
    # Create two columns for side-by-side charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Create a scatter plot for all partitions
        fig_all = px.scatter(
            file_size_df,
            x='partition_key',
            y='file_size_mb',
            color='partition_key',
            size='file_size_mb',
            size_max=15,
            opacity=0.7,
            title=f'Distribution Across All Partitions',
            hover_data=['file_path', 'file_size_mb', 'deviation_from_overall_avg']
        )
        
        # Add horizontal line for overall average
        fig_all.add_hline(
            y=overall_avg,
            line_color='red',
            line_width=2,
            line_dash='dash',
            annotation_text='Overall Avg',
            annotation_position='right'
        )
        
        # Add horizontal lines for each partition average
        for idx, row in partition_avg.iterrows():
            fig_all.add_shape(
                type="line",
                x0=idx-0.4,
                x1=idx+0.4,
                y0=row['avg_file_size_mb'],
                y1=row['avg_file_size_mb'],
                line=dict(color="green", width=1.5)
            )
        
        # Improve layout
        fig_all.update_layout(
            xaxis_title="Partition Key",
            yaxis_title="File Size (MB)",
            legend_title="Partition Key",
            height=500
        )
        
        st.plotly_chart(fig_all, use_container_width=True)
    
    with col2:
        # Display scatter plot of file sizes for selected partition
        fig_scatter = px.scatter(
            partition_files,
            x=partition_files.index,
            y='file_size_mb',
            color='deviation_from_partition_avg',
            color_continuous_scale='RdYlGn_r',
            size='file_size_mb',
            size_max=15,
            title=f'Deviation in Partition: {selected_partition}',
            hover_data=['file_path', 'file_size_mb', 'deviation_from_partition_avg']
        )
        
        # Add horizontal line for partition average
        fig_scatter.add_hline(
            y=partition_avg_size,
            line_color='green',
            line_width=2,
            line_dash='solid',
            annotation_text='Partition Avg',
            annotation_position='right'
        )
        
        # Add horizontal line for overall average
        fig_scatter.add_hline(
            y=overall_avg,
            line_color='red',
            line_width=2,
            line_dash='dash',
            annotation_text='Overall Avg',
            annotation_position='left'
        )
        
        # Improve layout
        fig_scatter.update_layout(
            xaxis_title="File Index",
            yaxis_title="File Size (MB)",
            coloraxis_colorbar_title="Deviation",
            height=500
        )
        
        st.plotly_chart(fig_scatter, use_container_width=True)

def display_snapshot_analysis(metrics_service, selected_table):
    """Display snapshot analysis with visualizations."""
    # Get snapshot details
    snapshot_df = metrics_service.get_table_snapshot_details(selected_table)
    
    if snapshot_df.empty:
        st.warning("No snapshot data available for this table.")
        return
    
    # Convert committed_at to datetime
    snapshot_df['committed_at'] = pd.to_datetime(snapshot_df['committed_at'])
    
    # Extract date and time components
    snapshot_df['date'] = snapshot_df['committed_at'].dt.date
    snapshot_df['hour'] = snapshot_df['committed_at'].dt.hour
    snapshot_df['day_of_week'] = snapshot_df['committed_at'].dt.day_name()
    
    # Add date range selector
    st.subheader("Select Date Range")
    
    # Get min and max dates from the data
    min_date = snapshot_df['date'].min()
    max_date = snapshot_df['date'].max()
    
    # Default to last 30 days if range is longer than that
    default_start = max_date - pd.Timedelta(days=30) if (max_date - min_date).days > 30 else min_date
    
    # Create date range selector
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", default_start, min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)
    
    # Filter data based on selected date range
    filtered_df = snapshot_df[(snapshot_df['date'] >= start_date) & (snapshot_df['date'] <= end_date)]
    
    # Show selected range info
    st.info(f"Showing data from {start_date} to {end_date} ({(end_date - start_date).days + 1} days)")
    
    # Add a section header for visualizations
    st.subheader("Snapshot Analysis")
    
    # Operations Over Time Chart
    st.subheader("Operations Over Time")
    
    # Group by date and operation, count occurrences
    operations_by_date = filtered_df.groupby(['date', 'operation']).size().reset_index(name='count')
    
    if operations_by_date.empty:
        st.warning("No operations data available for the selected date range.")
    else:
        # Create line chart with markers instead of area chart
        fig_operations = px.line(
            operations_by_date,
            x='date',
            y='count',
            color='operation',
            title='Operations Over Time',
            labels={'date': 'Date', 'count': 'Number of Snapshots', 'operation': 'Operation Type'},
            color_discrete_sequence=px.colors.qualitative.Bold,
            markers=True  # Add markers to the lines
        )
        
        # Customize the markers and lines
        fig_operations.update_traces(
            marker=dict(size=10, opacity=0.8),  # Larger, slightly transparent markers
            line=dict(width=3)  # Thicker lines
        )
        
        fig_operations.update_layout(
            xaxis_title="Date",
            yaxis_title="Number of Snapshots",
            height=400,  # Make the chart taller
            legend=dict(
                orientation="h",  # Horizontal legend
                yanchor="bottom",
                y=1.02,  # Position above the chart
                xanchor="right",
                x=1
            ),
            hovermode="x unified"  # Show all points at the same x-value when hovering
        )
        
        st.plotly_chart(fig_operations, use_container_width=True)
    
    # Add space between sections
    st.markdown("<div style='margin-top: 20px;'></div>", unsafe_allow_html=True)
    
    # Time Travel Section
    st.subheader("Time Travel")
    
    if filtered_df.empty:
        st.warning("No snapshots available in the selected date range.")
    else:
        # Sort by committed_at in descending order
        filtered_df = filtered_df.sort_values('committed_at', ascending=False)
        
        # Create radio buttons for operation type filtering
        operations = ["All Operations"] + sorted(filtered_df['operation'].unique().tolist())
        selected_operation = st.radio(
            "Filter by Operation Type",
            operations,
            horizontal=True
        )
        
        # Filter by operation if needed
        display_df = filtered_df
        if selected_operation != "All Operations":
            display_df = filtered_df[filtered_df['operation'] == selected_operation]
        
        # Create a dictionary mapping display strings to snapshot IDs
        snapshot_options = {}
        for _, row in display_df.iterrows():
            # Format the option string with timestamp, operation, and snapshot ID
            option_text = f"{row['committed_at'].strftime('%Y-%m-%d %H:%M:%S')} - {row['operation']} - ID: {row['snapshot_id']}"
            snapshot_options[option_text] = str(row['snapshot_id'])
        
        # Create a list of options for the selectbox
        option_list = list(snapshot_options.keys())
        
        # Add a default option
        option_list = ["Select a snapshot for time travel..."] + option_list
        
        # Create columns for the dropdown and button
        col1, col2 = st.columns([4, 1])
        
        with col1:
            # Create the selectbox
            selected_option = st.selectbox(
                "Select a snapshot for time travel:",
                options=option_list
            )
        
        # Initialize selected_snapshot_id and selected_details
        selected_snapshot_id = None
        selected_details = None
        
        # Check if a snapshot is selected
        if selected_option != "Select a snapshot for time travel...":
            # Get the snapshot ID from our dictionary
            selected_snapshot_id = snapshot_options[selected_option]
            
            # Get the details for this snapshot
            selected_row = display_df[display_df['snapshot_id'].astype(str) == selected_snapshot_id]
            
            if not selected_row.empty:
                selected_details = selected_row.iloc[0]
        
        with col2:
            # Add vertical alignment for the button
            st.markdown("<div style='margin-top: 32px;'></div>", unsafe_allow_html=True)
            
            # Time Travel Button (disabled if no snapshot is selected)
            button_disabled = selected_snapshot_id is None
            if st.button("Time Travel", key="time_travel_button", disabled=button_disabled, use_container_width=True):
                if selected_details is not None:
                    # This is a placeholder - replace with actual time travel implementation
                    metrics_service.time_travel_to_snapshot(selected_table, selected_snapshot_id)
                    st.success(f"Time traveled to snapshot {selected_snapshot_id}. The table is now at the state as of {selected_details['committed_at']}.")
        
        # Display details for the selected snapshot
        if selected_details is not None:
            # Create two columns for details
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("### Snapshot Details")
                st.markdown(f"**ID:** {selected_details['snapshot_id']}")
                st.markdown(f"**Time:** {selected_details['committed_at']}")
                st.markdown(f"**Operation:** {selected_details['operation']}")
                st.markdown(f"**Parent ID:** {selected_details.get('parent_id', 'N/A')}")
            
            with col2:
                st.markdown("### Data Changes")
                st.markdown(f"**Records Added:** {selected_details['added_records']}")
                st.markdown(f"**Records Deleted:** {selected_details['deleted_records']}")
                st.markdown(f"**Total Records:** {selected_details['total_records']}")
                st.markdown(f"**Total Files:** {selected_details['total_data_files']}")
                
                # Try to display file size if available
                try:
                    file_size = selected_details['total_file_size']
                    if isinstance(file_size, (int, float)):
                        st.markdown(f"**Total Size:** {file_size/1024/1024:.2f} MB")
                    else:
                        st.markdown(f"**Total Size:** {file_size}")
                except:
                    pass

def display_column_metrics(metrics_service, selected_table):
    """Display column-level metrics with interactive table and conditional formatting."""
    # Get column metrics data
    column_metrics_df = metrics_service.get_column_level_metrics(selected_table)
    
    if column_metrics_df.empty:
        st.warning("No column-level metrics available for this table.")
        return
    
    # Add section header
    display_section_header("Column-Level Analysis")
    
    # Calculate null percentage
    if 'null_count' in column_metrics_df.columns:
        # Get total records from table metrics
        table_metrics = metrics_service.get_all_table_metrics()
        if not table_metrics.empty:
            selected_metrics = table_metrics[table_metrics['source_table'] == selected_table]
            if not selected_metrics.empty:
                total_records = selected_metrics.iloc[0]['active_total_records']
                column_metrics_df['null_percentage'] = (column_metrics_df['null_count'] / total_records * 100).round(2)
    
    # Prepare table display
    table_display = column_metrics_df.copy()
    table_display['total_size_mb'] = table_display['total_size_mb'].round(2)
    
    # Rename columns for display
    table_display = table_display.rename(columns={
        'column_name': 'Column Name',
        'data_type': 'Data Type',
        'required': 'Required',
        'total_size_mb': 'Size (MB)',
        'null_count': 'Null Count'
    })
    
    if 'null_percentage' in table_display.columns:
        table_display['Null %'] = table_display['null_percentage'].round(2)
        table_display = table_display.drop(columns=['null_percentage'])
    
    # Drop column_id and total_size_bytes for cleaner display
    if 'column_id' in table_display.columns:
        table_display = table_display.drop(columns=['column_id'])
    if 'total_size_bytes' in table_display.columns:
        table_display = table_display.drop(columns=['total_size_bytes'])
    
    # Add filtering options
    col1, col2 = st.columns(2)
    
    with col1:
        # Add filter by data type
        data_types = ['All Types'] + sorted(table_display['Data Type'].unique().tolist())
        selected_type = st.selectbox("Filter by Data Type", data_types)
    
    with col2:
        # Add filter for required columns
        required_options = ['All Columns', 'Required Only', 'Optional Only']
        selected_required = st.selectbox("Filter by Required Status", required_options)
    
    # Apply filters
    filtered_df = table_display.copy()
    
    if selected_type != 'All Types':
        filtered_df = filtered_df[filtered_df['Data Type'] == selected_type]
    
    if selected_required == 'Required Only':
        filtered_df = filtered_df[filtered_df['Required'] == True]
    elif selected_required == 'Optional Only':
        filtered_df = filtered_df[filtered_df['Required'] == False]
    
    # Add search box
    search_term = st.text_input("Search Columns", "")
    if search_term:
        filtered_df = filtered_df[filtered_df['Column Name'].str.contains(search_term, case=False)]
    
    # Define styling function for conditional formatting
    def style_dataframe(df):
        # Create a copy to avoid modifying the original
        styled_df = df.copy()
        
        # Define style functions
        def highlight_size(val):
            """Highlight large column sizes with gradient colors"""
            if isinstance(val, (int, float)):
                # Get percentile rank of this value
                max_val = df['Size (MB)'].max()
                if max_val > 0:
                    normalized = val / max_val
                    # Create a blue gradient from light to dark
                    color = f'rgba(65, 105, 225, {min(normalized + 0.1, 0.9)})'
                    return f'background-color: {color}; color: white;'
            return ''
        
        def highlight_null_percentage(val):
            """Highlight high null percentages with gradient colors"""
            if isinstance(val, (int, float)):
                if val > 50:  # More than 50% nulls
                    intensity = min(val / 100 + 0.3, 0.9)
                    return f'background-color: rgba(220, 53, 69, {intensity}); color: white;'
                elif val > 20:  # 20-50% nulls
                    intensity = min(val / 100 + 0.1, 0.7)
                    return f'background-color: rgba(255, 193, 7, {intensity});'
            return ''
        
        def highlight_required(val):
            """Highlight required columns"""
            if val == True:
                return 'background-color: rgba(40, 167, 69, 0.2);'
            return ''
        
        # Apply styles
        styled = styled_df.style.applymap(highlight_size, subset=['Size (MB)'])
        
        if 'Null %' in styled_df.columns:
            styled = styled.applymap(highlight_null_percentage, subset=['Null %'])
        
        styled = styled.applymap(highlight_required, subset=['Required'])
        
        return styled
    
    # Apply styling and display the table
    styled_df = style_dataframe(filtered_df)
    
    # Display summary statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "Total Columns", 
            len(filtered_df),
            f"{len(filtered_df) - len(table_display)} from filter" if len(filtered_df) != len(table_display) else None
        )
    with col2:
        st.metric(
            "Total Size", 
            f"{filtered_df['Size (MB)'].sum():.2f} MB",
            f"{(filtered_df['Size (MB)'].sum() / table_display['Size (MB)'].sum() * 100):.1f}% of total" if len(filtered_df) != len(table_display) else None
        )
    with col3:
        if 'Null %' in filtered_df.columns:
            avg_null = filtered_df['Null %'].mean()
            st.metric(
                "Avg Null %", 
                f"{avg_null:.2f}%",
                None
            )
    
    # Display the styled table
    st.dataframe(
        styled_df,
        use_container_width=True,
        height=500,
        hide_index=True
    )
    
    # Add a bar chart showing top columns by size
    st.subheader("Top Columns by Size")
    
    # Sort and get top 10 columns by size
    top_columns = filtered_df.sort_values('Size (MB)', ascending=False).head(10)
    
    # Create bar chart
    fig = px.bar(
        top_columns,
        x='Column Name',
        y='Size (MB)',
        color='Data Type',
        color_discrete_sequence=px.colors.qualitative.Bold,
        title='Top 10 Columns by Storage Size',
        labels={'Size (MB)': 'Storage Size (MB)'}
    )
    
    fig.update_layout(
        xaxis_title="Column Name",
        yaxis_title="Size (MB)",
        xaxis={'categoryorder': 'total descending'},
        legend_title="Data Type",
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)

def sync_button(metrics_service):
    """
    Create a sync button in the top right corner to trigger metadata sync.
    """
    # Create container for button in top right
    with st.container():
        col1, col2, col3 = st.columns([5, 1, 1])
        
        with col2:
            if st.button("💬 Chat", help="Open AI chat assistant"):
                st.session_state.show_chat = True
        
        with col3:
            if st.button("🔄 Sync", help="Sync metadata to database"):
                with st.spinner("Syncing metadata..."):
                    try:
                        # Call the sync method directly
                        metrics_service.copy_metrics_to_db()
                        
                        # Show success message
                        st.success("Metadata synced successfully!")
                    except Exception as e:
                        st.error(f"Failed to sync metadata: {str(e)}")

def display_table_recommendations(recommendation_service, selected_table):
    """Display recommendations for a specific table."""
    recommendations = recommendation_service.get_table_recommendations(selected_table)
    
    if not recommendations:
        st.info("No recommendations available for this table at this time.")
        return
    
    # Display recommendations in a grid layout
    cols = st.columns(2)
    for i, rec in enumerate(recommendations):
        with cols[i % 2]:
            display_recommendation_card(rec, is_global=False)

def display_recommendation_card(recommendation, is_global=False):
    """Display a single recommendation card with action buttons."""
    # Define icon based on recommendation type
    icon_map = {
        'compact': '🗜️',
        'vacuum': '🧹',
        'optimize': '⚡',
        'partition': '📊',
        'alert': '⚠️',
        'info': 'ℹ️'
    }
    
    rec_type = recommendation.get('type', 'info')
    icon = icon_map.get(rec_type, 'ℹ️')
    
    # Define color based on priority
    priority_colors = {
        'high': '#e74c3c',
        'medium': '#f39c12',
        'low': '#3498db',
        'info': '#2ecc71'
    }
    
    priority = recommendation.get('priority', 'info')
    color = priority_colors.get(priority, '#3498db')
    
    # Create card with recommendation
    with st.container():
        st.markdown(f"""
            <div style="
                border-left: 5px solid {color};
                background: linear-gradient(135deg, rgba(239, 248, 255, 0.9) 0%, rgba(230, 244, 255, 0.8) 100%);
                border-radius: 10px;
                padding: 15px 20px;
                margin: 15px 0;
                box-shadow: 0 4px 10px rgba(0, 0, 0, 0.05);
            ">
                <div style="display: flex; align-items: center;">
                    <div style="font-size: 24px; margin-right: 15px;">{icon}</div>
                    <div>
                        <h4 style="margin: 0; color: #2c3e50;">{recommendation['title']}</h4>
                        <p style="margin: 5px 0 10px 0; color: #34495e;">{recommendation['description']}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        # Add action buttons if available
        if 'actions' in recommendation:
            cols = st.columns(len(recommendation['actions']))
            for i, action in enumerate(recommendation['actions']):
                with cols[i]:
                    if st.button(action['label'], key=f"action_{action['id']}"):
                        # Execute the action
                        try:
                            result = action['function']()
                            st.success(f"Action completed: {result}")
                        except Exception as e:
                            st.error(f"Error executing action: {str(e)}")

def initialize_chat_agent(metrics_service):
    """Initialize the chat agent with the metrics service."""
    return Assistant(metrics_service)

def display_chat_window(assistant, selected_table=None):
    """Display the chat window with snowman theme."""
    # Custom CSS for snowman theme
    st.markdown("""
        <style>
        /* Chat container */
        .chat-container {
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 400px;
            height: 500px;
            background: linear-gradient(135deg, #e0f0ff 0%, #ffffff 100%);
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            z-index: 1000;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        
        /* Chat header */
        .chat-header {
            background: linear-gradient(90deg, #2B6CB0, #63B3ED);
            color: white;
            padding: 10px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        /* Close button */
        .chat-close {
            cursor: pointer;
            font-weight: bold;
        }
        
        /* Chat message styling */
        .stChatMessage {
            background: rgba(255, 255, 255, 0.9);
            border-radius: 15px;
            padding: 10px;
            margin: 5px 0;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        
        /* Chat input box styling */
        .stChatInputContainer {
            background-color: rgba(255, 255, 255, 0.9) !important;
            border-radius: 20px !important;
            padding: 5px !important;
            border: 2px solid #a8d5ff !important;
        }
        
        /* Change assistant icon to snowman */
        .stChatMessageIcon {
            content: "⛄";
            font-size: 24px;
            color: #2c3e50;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Create chat container
    chat_container = st.container()
    
    with chat_container:
        # Chat header with close button
        col1, col2 = st.columns([5, 1])
        with col1:
            st.markdown("### ⛄ Iceberg AI Assistant")
        with col2:
            if st.button("✖️", key="close_chat"):
                st.session_state.show_chat = False
                st.experimental_rerun()
        
        # Add context about selected table if available
        if selected_table:
            st.info(f"Currently analyzing table: {selected_table}")
        
        # Initialize chat history
        if "messages" not in st.session_state:
            st.session_state.messages = []
            # Add welcome message
            st.session_state.messages.append({
                "role": "assistant", 
                "content": "Hello! I'm your Iceberg AI Assistant. How can I help you with your Iceberg tables today?"
            })

        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        # Chat input
        if prompt := st.chat_input("Ask me anything about your Iceberg tables..."):
            # Add context about selected table if available
            if selected_table:
                context_prompt = f"For table {selected_table}: {prompt}"
            else:
                context_prompt = prompt
                
            # Add user message to chat history
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            # Get AI response
            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        response = assistant.ask(context_prompt)
                        st.markdown(response)
                        st.session_state.messages.append({"role": "assistant", "content": response})
                    except Exception as e:
                        error_message = f"An error occurred: {str(e)}"
                        st.error(error_message)
                        st.session_state.messages.append({"role": "assistant", "content": error_message})

def render_dashboard():
    metrics_service = MetricsService()
    
    # Track current page/table to reset chat when changed
    current_page = st.session_state.get("current_page", None)
    
    sync_button(metrics_service)
    tables = metrics_service.list_tables()
    tables_df = pd.DataFrame(tables, columns=['source_table'])

    apply_sidebar_style()
    selected_table = render_sidebar(tables_df)
    
    # Check if page/table has changed
    new_page = selected_table if selected_table else "home"
    if current_page != new_page:
        # Clear chat history when page changes
        if "messages" in st.session_state:
            del st.session_state.messages
        # Update current page
        st.session_state.current_page = new_page

    # Add recommendation service
    recommendation_service = RecommendationService(metrics_service)
    
    # Create assistant only when needed (to save costs)
    assistant = None
    
    table_metrics = metrics_service.get_all_table_metrics()

    if not selected_table:
        namespace_statistics = metrics_service.get_namespace_statistics()
        display_overall_metrics(namespace_statistics)
        
        # Get and display table metrics
        if table_metrics is not None:
            display_table_metrics(table_metrics)
        
        # No global recommendations here

    if selected_table:
        table_metrics = metrics_service.get_all_table_metrics()
        selected_metrics = table_metrics[table_metrics['source_table'] == selected_table].iloc[0]
        
        snapshot_details = metrics_service.get_table_snapshot_details(selected_table)
        oldest_snapshot_date = snapshot_details['committed_at'].min()
        latest_snapshot_date = snapshot_details['committed_at'].max()
        total_snapshots = len(snapshot_details)
        average_file_size = selected_metrics['avg_file_size_mb']

        st.markdown(f"""
            <h1 style='
                color: #2C5282;
                text-align: center;
                margin-top: 3rem;
                margin-bottom: 4rem;
                padding-top: 2rem;
                font-size: 3.2rem;
                background: linear-gradient(45deg, #2B6CB0, #63B3ED);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                letter-spacing: 0.5px;
            '>{selected_table}</h1>
        """, unsafe_allow_html=True)

        # Convert metrics list to format needed for display_metrics_grid
        metrics_data = [
            {
                "label": "<b>Active/Total Files</b>",
                "value": f"{selected_metrics['active_files']:,} / {selected_metrics['total_files']:,}",
                "color": "#3498db"
            },
            {
                "label": "<b>Storage Usage (MB)</b>",
                "value": f"{selected_metrics['active_size_mb']:,.1f}/{selected_metrics['total_size_mb']:,.1f}",
                "color": "#2ecc71"
            },
            {
                "label": "<b>Active/Total Records</b>",
                "value": f"{selected_metrics['active_total_records']:,} / {selected_metrics['all_total_records']:,}",
                "color": "#9b59b6"
            },
            {
                "label": "<b>First Snapshot</b>",
                "value": oldest_snapshot_date.strftime('%Y-%m-%d'),
                "color": "#e67e22"
            },
            {
                "label": "<b>Last Snapshot</b>",
                "value": latest_snapshot_date.strftime('%Y-%m-%d'),
                "color": "#e74c3c"
            },
            {
                "label": "<b>Total Snapshots</b>",
                "value": f"{total_snapshots:,}",
                "color": "#1abc9c"
            },
            {
                "label": "<b>Average File Size</b>",
                "value": f"{average_file_size:,.1f} MB",
                "color": "#34495e"
            }
        ]

        display_selected_table_metrics(metrics_data)
        
        display_section_header("Table Growth Analysis")
        daily_metrics_df = get_filtered_metrics_data(metrics_service, selected_table)
        records_chart, files_chart, size_chart = create_growth_charts(daily_metrics_df)
        display_growth_metrics(daily_metrics_df)
        display_growth_charts(records_chart, files_chart, size_chart)

        display_section_header("Partition Analysis")
        display_partition_analysis(metrics_service, selected_table)
        display_file_size_analysis(metrics_service, selected_table)
        
        # Add Snapshot Analysis section
        display_section_header("Snapshot Analysis")
        display_snapshot_analysis(metrics_service, selected_table)
        
        # Display column metrics
        display_column_metrics(metrics_service, selected_table)
        
        # Display Smart Recommendations - Force display even if no recommendations
        display_section_header("Smart Recommendations")
        try:
            # Try to get recommendations
            recommendations = recommendation_service.get_table_recommendations(selected_table)
            
            if recommendations:
                # Display recommendations in a grid layout
                cols = st.columns(2)
                for i, rec in enumerate(recommendations):
                    with cols[i % 2]:
                        display_recommendation_card(rec, is_global=False)
            else:
                # Show message if no recommendations
                st.info("No recommendations available for this table at this time.")
        except Exception as e:
            # Show error if recommendation service fails
            st.error(f"Error loading recommendations: {str(e)}")
            st.info("Smart recommendations will be available soon.")
        
        # Display Chat right below Smart Recommendations - Always show
        st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)
        display_section_header("AI Chat Assistant")
        
        # Create assistant only when needed (lazy initialization)
        if assistant is None:
            try:
                assistant = Assistant(metrics_service)
                display_chat_window(assistant, selected_table)
            except Exception as e:
                st.error(f"Error loading chat assistant: {str(e)}")
                st.info("Chat assistant will be available soon.")

   
   