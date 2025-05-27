import pandas as pd
import glob
import os



# List of output folders created by Spark
folder_names = ["controversial_comment.csv", "gilded_comment.csv", "hot_comment.csv","new_comment.csv", "top_comment.csv", "random_rising_comment.csv"
]



base_input_dir = "/home/avallala2023/Desktop/Combined/collected_files/comments"
base_output_dir = "/home/avallala2023/Desktop/Combined/collected_files/comments/cleaned"



for folder in folder_names:
	sort_by = folder.split('_')[0]
	folder_path = os.path.join(base_input_dir, folder)
	
	# Read all part-*.csv files inside the folder
	part_files = glob.glob(f"{folder_path}/part-*.csv")
	if not part_files:
		print(f"⚠️ No part files found in {folder_path}")
		continue
	# Combine all part files into one DataFrame
	df = pd.concat((pd.read_csv(f, on_bad_lines='skip', quoting=3, engine='python') for f in part_files),ignore_index=True)
 
 
	# Add new column BEFORE cleaning
	df["sort_by"] = sort_by
	
	# Clean the DataFrame
	df = df.dropna(subset=["comment_id"])
	df = df.sort_values(by=["comment_id", "comment_upvotes"], ascending=[True, False])
	df = df.drop_duplicates(subset="comment_id", keep="first")

	# Output path
	os.makedirs(base_output_dir, exist_ok=True)
	output_file = os.path.join(base_output_dir, folder.replace(".csv", "_cleaned.csv"))
	df.to_csv(output_file, index=False)
	print(f"✅ Cleaned file saved: {output_file}")
 
