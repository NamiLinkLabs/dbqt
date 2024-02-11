# DBQT (DataBase Quality Tool) 🎯

DBQT is a lightweight, Python-first data quality testing framework that helps data teams maintain high-quality data through automated checks and intelligent suggestions. 

## 🛠️ Current Tools

### Column Comparison Tool (dbqt compare)
Compare database schemas between source and target databases:
- Table-level comparison
- Column-level comparison with data type compatibility checks
- Generates detailed Excel report with:
  - Table differences
  - Column differences
  - Data type mismatches
  - Formatted worksheets for easy analysis

Usage:
```bash
dbqt compare source_schema.csv target_schema.csv
```

### Database Statistics Tool (dbqt dbstats)
Collect and analyze database statistics:
- Table row counts
- Updates statistics in CSV format
- Configurable through YAML

Usage:
```bash
dbqt dbstats config.yaml
```

## 🚀 Future Plans

### Core DBQT Features (Coming Soon)
- AI-Powered column classification using Qwen2 0.5B
- Automatic check suggestions
- 20+ built-in data quality checks
- Python-first API
- No backend required
- Customizable check framework

### Planned Checks
- Completeness checks (null values)
- Uniqueness validation
- Format validation (regex, dates, emails)
- Range/boundary checks
- Value validation
- Statistical analysis
- Dependency checks

### Integration Plans
- Data pipeline integration
- Scheduled runs
- Parallel check execution
- Multiple database backend support

## 📄 License

This project is licensed under the MIT License.
