import pandas as pd
from datetime import datetime, timedelta

class RecommendationService:
    """Service for generating smart recommendations based on table metrics."""
    
    def __init__(self, metrics_service):
        """Initialize with metrics service for data access."""
        self.metrics_service = metrics_service
    
    def get_global_recommendations(self, table_metrics_df):
        """Generate recommendations for all tables."""
        recommendations = []
        
        # Skip if no metrics available
        if table_metrics_df is None or table_metrics_df.empty:
            return recommendations
        
        # Find tables with high waste (difference between total and active size)
        waste_threshold = 0.5  # 50% waste
        high_waste_tables = []
        
        for _, row in table_metrics_df.iterrows():
            if row['total_size_mb'] > 0:
                waste_ratio = (row['total_size_mb'] - row['active_size_mb']) / row['total_size_mb']
                if waste_ratio > waste_threshold and row['total_size_mb'] > 100:  # Only consider tables > 100MB
                    high_waste_tables.append({
                        'table': row['source_table'],
                        'waste_ratio': waste_ratio,
                        'total_size': row['total_size_mb'],
                        'active_size': row['active_size_mb']
                    })
        
        # Sort by waste ratio (highest first)
        high_waste_tables.sort(key=lambda x: x['waste_ratio'], reverse=True)
        
        # Create recommendation for tables with high waste
        if high_waste_tables:
            top_tables = high_waste_tables[:3]  # Top 3 wasteful tables
            table_list = ", ".join([t['table'] for t in top_tables])
            
            # Calculate potential space savings
            total_waste = sum([t['total_size'] - t['active_size'] for t in top_tables])
            
            recommendations.append({
                'type': 'vacuum',
                'priority': 'high',
                'title': 'Storage Optimization Opportunity',
                'description': f"Tables {table_list} have significant wasted space. Running vacuum could free up approximately {total_waste:.1f} MB.",
                'actions': [
                    {
                        'id': 'vacuum_all',
                        'label': 'Vacuum All',
                        'function': lambda: self._vacuum_tables([t['table'] for t in top_tables])
                    }
                ]
            })
        
        # Find tables with many small files
        small_file_tables = []
        for _, row in table_metrics_df.iterrows():
            if row['total_files'] > 100 and row['avg_file_size_mb'] < 10:  # Many files, small average size
                small_file_tables.append({
                    'table': row['source_table'],
                    'file_count': row['total_files'],
                    'avg_size': row['avg_file_size_mb']
                })
        
        # Sort by file count (highest first)
        small_file_tables.sort(key=lambda x: x['file_count'], reverse=True)
        
        # Create recommendation for tables with many small files
        if small_file_tables:
            top_tables = small_file_tables[:3]  # Top 3 tables with small files
            table_list = ", ".join([t['table'] for t in top_tables])
            
            recommendations.append({
                'type': 'compact',
                'priority': 'medium',
                'title': 'File Compaction Recommended',
                'description': f"Tables {table_list} have many small files. Compacting these tables could improve query performance.",
                'actions': [
                    {
                        'id': 'compact_all',
                        'label': 'Compact All',
                        'function': lambda: self._compact_tables([t['table'] for t in top_tables])
                    }
                ]
            })
        
        # Add general recommendation if no specific ones
        if not recommendations:
            recommendations.append({
                'type': 'info',
                'priority': 'info',
                'title': 'All Systems Optimal',
                'description': "No specific recommendations at this time. Your tables appear to be well-maintained."
            })
        
        return recommendations
    
    def get_table_recommendations(self, table_name):
        """Generate recommendations for a specific table."""
        recommendations = []
        
        # Get table metrics
        table_metrics = self.metrics_service.get_all_table_metrics()
        if table_metrics is None or table_metrics.empty:
            return recommendations
        
        selected_metrics = table_metrics[table_metrics['source_table'] == table_name]
        if selected_metrics.empty:
            return recommendations
        
        selected_metrics = selected_metrics.iloc[0]
        
        # Get partition details
        partition_df = self.metrics_service.get_partition_details(table_name)
        
        # Get growth data
        growth_data = self.metrics_service.get_table_growth_analysis(table_name)
        if growth_data is not None and not growth_data.empty:
            growth_data['date'] = pd.to_datetime(growth_data['date'])
        
        # Check for storage waste
        if selected_metrics['total_size_mb'] > 0:
            waste_ratio = (selected_metrics['total_size_mb'] - selected_metrics['active_size_mb']) / selected_metrics['total_size_mb']
            if waste_ratio > 0.3:  # 30% waste
                waste_mb = selected_metrics['total_size_mb'] - selected_metrics['active_size_mb']
                recommendations.append({
                    'type': 'vacuum',
                    'priority': 'high' if waste_ratio > 0.5 else 'medium',
                    'title': 'Storage Waste Detected',
                    'description': f"This table has {waste_ratio:.1%} wasted space ({waste_mb:.1f} MB). Running vacuum could reclaim this space.",
                    'actions': [
                        {
                            'id': 'vacuum_table',
                            'label': 'Run Vacuum',
                            'function': lambda: self.metrics_service.run_vacuum(table_name)
                        }
                    ]
                })
        
        # Check for small files
        if selected_metrics['avg_file_size_mb'] < 10 and selected_metrics['total_files'] > 50:
            recommendations.append({
                'type': 'compact',
                'priority': 'medium',
                'title': 'Small Files Detected',
                'description': f"This table has {selected_metrics['total_files']} files with an average size of {selected_metrics['avg_file_size_mb']:.1f} MB. Compacting could improve query performance.",
                'actions': [
                    {
                        'id': 'compact_table',
                        'label': 'Run Compaction',
                        'function': lambda: self.metrics_service.run_compaction_job(table_name)
                    }
                ]
            })
        
        # Check partition skew if partition data available
        if partition_df is not None and not partition_df.empty and len(partition_df) > 1:
            # Calculate coefficient of variation for partition sizes
            mean_size = partition_df['total_size_mb'].mean()
            std_size = partition_df['total_size_mb'].std()
            if mean_size > 0:
                cv = std_size / mean_size
                if cv > 1.5:  # High variation
                    # Find largest partition
                    largest_partition = partition_df.loc[partition_df['total_size_mb'].idxmax()]
                    recommendations.append({
                        'type': 'partition',
                        'priority': 'medium',
                        'title': 'Partition Size Imbalance',
                        'description': f"Partition '{largest_partition['partition_key']}' is significantly larger than others. Consider reviewing your partitioning strategy.",
                        'actions': []  # No automatic action for this
                    })
        
        # Check growth rate if growth data available
        if growth_data is not None and not growth_data.empty and len(growth_data) > 7:  # At least a week of data
            # Calculate recent growth rate (last 7 days)
            recent_data = growth_data.sort_values('date', ascending=False).head(7)
            if len(recent_data) > 1:
                first_size = recent_data.iloc[-1]['total_file_size']
                last_size = recent_data.iloc[0]['total_file_size']
                days = (recent_data.iloc[0]['date'] - recent_data.iloc[-1]['date']).days
                if days > 0 and first_size > 0:
                    growth_rate = (last_size - first_size) / first_size / days  # Daily growth rate
                    
                    if growth_rate > 0.1:  # More than 10% daily growth
                        # Project size in 30 days
                        projected_size = last_size * (1 + growth_rate) ** 30
                        recommendations.append({
                            'type': 'alert',
                            'priority': 'high',
                            'title': 'Rapid Growth Detected',
                            'description': f"This table is growing at {growth_rate:.1%} per day. At this rate, it will reach {projected_size:.1f} MB in 30 days.",
                            'actions': []  # No automatic action for this
                        })
        
        # Add optimize recommendation if no specific ones
        if not recommendations:
            recommendations.append({
                'type': 'info',
                'priority': 'info',
                'title': 'Table Appears Healthy',
                'description': "No specific recommendations at this time. This table appears to be well-maintained."
            })
        
        return recommendations
    
    def _vacuum_tables(self, table_list):
        """Run vacuum on multiple tables."""
        results = []
        for table in table_list:
            try:
                self.metrics_service.run_vacuum(table)
                results.append(f"Vacuum started for {table}")
            except Exception as e:
                results.append(f"Failed to vacuum {table}: {str(e)}")
        
        return "; ".join(results)
    
    def _compact_tables(self, table_list):
        """Run compaction on multiple tables."""
        results = []
        for table in table_list:
            try:
                self.metrics_service.run_compaction_job(table)
                results.append(f"Compaction started for {table}")
            except Exception as e:
                results.append(f"Failed to compact {table}: {str(e)}")
        
        return "; ".join(results) 