# Keyword-Based CSV Processor using PySpark

This PySpark script scans multiple CSV files, filters rows based on AI and Education-related keywords, and saves the filtered results into categorized output files. It is optimized to work on large structured/unstructured datasets distributed across directories.

---

## 📌 What This Code Does

This script performs the following operations:

1. **Starts a PySpark Session**:
   - Initializes a Spark application to leverage distributed computing.

2. **Defines Keyword Lists**:
   - Two sets of keywords are defined and broadcasted for efficiency:
     - Education-related (e.g., `school`, `student`, `teacher`)
     - AI-related (e.g., `chatgpt`, `ai`, `generative ai`)

3. **Searches for All CSV Files**:
   - Recursively looks through a specified input directory and gathers all `.csv` files.

4. **Processes Each File**:
   - Reads each file as a Spark DataFrame.
   - Combines all text columns into one single string column for keyword search.
   - Cleans text using regular expressions and converts to lowercase.

5. **Applies Keyword Filters**:
   - For each row, checks if the combined text contains:
     - Only education keywords → saves in `edu` folder
     - Only AI keywords → saves in `ai` folder
     - Both AI and education keywords → saves in `edu_ai` folder

6. **Saves Filtered Data**:
   - Writes the filtered rows into new CSV files in a categorized output directory with naming like:
     ```
     originalfilename_matched_ai.csv
     originalfilename_matched_edu.csv
     originalfilename_matched_edu_ai.csv
     ```

7. **Handles Errors Gracefully**:
   - Skips files that cannot be read and prints an error message for debugging.

---

## 🔍 Features

- ✅ Efficient text filtering using broadcast variables and UDFs
- 📂 Handles nested directories and large datasets
- 🧹 Cleans and standardizes text before filtering
- 💾 Categorizes output files based on matched keyword group

---

## 🧠 Keywords

- **Education Keywords**:
  - `curriculum`, `tutoring`, `education`, `school`, `university`, `student`, `teacher`, `kindergarten`, `elementary school`, `middle school`, `high school`, etc.
- **AI Keywords**:
  - `generative ai`, `gen ai`, `chatgpt`, `ai`, `midjourney`, `chatgpt4o`

---

## 🛠️ Requirements

- Python 3.x
- Apache Spark with PySpark
- A Unix-like OS (macOS/Linux)

Install dependencies:
```bash
pip install pyspark
```
