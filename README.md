# DBQT (DataBase Quality Tool) 🎯

DBQT is a lightweight, Python-first data quality testing framework that helps data teams maintain high-quality data through automated checks and intelligent suggestions. Powered by Qwen2 0.5B small language model for smart column classification and check recommendations.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Key Features

- **No Backend Required**: Run locally with SQLite, or scale up as needed
- **AI-Powered**: Automatic column classification and check suggestions using Qwen2 0.5B
- **Python-First**: Native Python API for seamless integration with existing data pipelines
- **Extensive Check Library**: 20+ built-in data quality checks
- **Easy to Deploy**: Simple pip install, no Docker needed
- **Customizable**: Extend with your own custom checks

## 🛠️ Installation

```bash
pip install dbqt
```

## 🏃 Quick Start

```python
from dbqt import DBQT
import polars as pl

# Initialize DBQT
dbqt = DBQT()

# Load your data
df = pl.read_csv("your_data.csv")

# Get automatic check suggestions
suggested_checks = dbqt.suggest_checks(df)

# Run checks
results = dbqt.run_checks(df, suggested_checks)

# View results
print(results.summary())
```

## 📊 Available Checks

### Completeness
- `not_null`: Check for null/missing values

### Uniqueness
- `unique`: Single column uniqueness
- `unique_combination`: Multi-column uniqueness

### Format Validation
- `regex_match`: Pattern matching
- `date_format`: Valid date format
- `timestamp_format`: Valid timestamp format
- `email_format`: Valid email format
- `phone_format`: Valid phone number format
- `numeric_format`: Valid number format
- `json_format`: Valid JSON structure

### Range/Boundary
- `min_value`: Minimum value check
- `max_value`: Maximum value check
- `value_between`: Value range validation
- `no_future_dates`: Date not in future
- `min_length`: Minimum string length
- `max_length`: Maximum string length

### Value Validation
- `in_domain`: Value from allowed set
- `ref_integrity`: Referential integrity
- `positive_only`: Positive numbers only
- `consistent_casing`: Uniform text casing

### Statistical
- `stat_outliers`: Statistical outlier detection
- `value_distribution`: Distribution analysis
- `trend_check`: Trend monitoring

### Dependency Checks
- `dependent_column_check`: Validate dependencies between columns

## 📝 Example Configuration

```python
checks = {
    "user_id": [
        {"check": "not_null"},
        {"check": "unique"},
        {"check": "regex_match", "pattern": r"^USER_\d+$"}
    ],
    "email": [
        {"check": "not_null"},
        {"check": "email_format"},
        {"check": "unique"}
    ],
    "age": [
        {"check": "numeric_format"},
        {"check": "value_between", "min": 0, "max": 120},
        {"check": "stat_outliers"}
    ]
}
```

## 🔍 AI-Powered Suggestions

DBQT uses Qwen2 0.5B to:
- Analyze column names and sample data
- Classify column types and purposes
- Suggest appropriate quality checks
- Recommend validation rules

## 📈 Scaling Up

While DBQT works great with SQLite for smaller datasets, it can be scaled up by:
- Using a production database backend
- Implementing parallel check execution
- Setting up scheduled runs
- Integrating with your data pipeline

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.

## 🙏 Acknowledgments

Inspired by [MobyDQ](https://ubisoft.github.io/mobydq/), but reimagined as a lightweight, Python-first solution with AI capabilities.
