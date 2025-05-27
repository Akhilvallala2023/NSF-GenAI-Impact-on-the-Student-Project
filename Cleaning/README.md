# Reddit Data Cleaner (Posts and Comments)

This repository includes two Python scripts that process raw Reddit data (collected in parts) and output cleaned datasets for posts and comments. These scripts are optimized for data pipelines where Reddit data is initially stored in split CSV files from distributed systems like Apache Spark.

---

## 📜 Files Included

- `cleaned_posts.py`: Processes and cleans post data files.
- `cleaned_comments.py`: Processes and cleans comment data files.

---

## 🧠 What These Scripts Do

### 🔹 `cleaned_posts.py`

1. **Input Directory**: Scans through the following folder:
/home/.../posts/

for subfolders named like `controversial_post.csv`, `hot_post.csv`, etc.

2. **Reads all `part-*.csv` files** inside each subfolder (these are usually Spark output chunks).

3. **Combines** all parts into a single DataFrame using `pandas`.

4. **Adds a new column** called `sort_by` which stores the type of Reddit sort (e.g., "hot", "top", etc.).

5. **Cleans the DataFrame** by:
- Dropping rows with null `post_id`s.
- Sorting by `created_at` if applicable.
- Resetting the index.

6. **Saves cleaned output** to:
/home/.../posts/cleaned/


---

### 🔹 `cleaned_comments.py`

1. **Input Directory**: Scans through the following folder:
/home/.../comments/

for subfolders named like `hot_comment.csv`, `top_comment.csv`, etc.

2. **Reads all `part-*.csv` files** inside each subfolder.

3. **Combines** the parts into a single DataFrame.

4. **Adds a new column** called `sort_by` which stores the comment type.

5. **Cleans the DataFrame** by:
- Dropping rows with null `comment_id`s.
- Sorting and reindexing.

6. **Saves cleaned output** to:
/home/.../comments/cleaned/


---

## 📁 Folder Structure (Before and After)

### Input:
collected_files/
├── posts/
│ ├── hot_post.csv/
│ │ └── part-0000.csv
│ └── ...
└── comments/
├── top_comment.csv/
│ └── part-0000.csv
└── ...


### Output:
collected_files/
├── posts/
│ └── cleaned/
│ └── hot_post.csv
└── comments/
└── cleaned/
└── top_comment.csv


---

## 🛠️ Requirements

- Python 3.x
- pandas

Install dependencies:

```bash
pip install pandas
```
