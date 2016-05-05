
README --- Anchor Point --- Hengte Lin
---------------------------------------
codes in this folder are used to implement anchor point strategy on Spark using Scala.
To reproduce the work, please follow the instruction below:
1.clean the data and save them into csv files as below:
	data/prescriptions.csv - subject_id, drug
	data/patients.csv      - subject_id, gender, admittime, dob
	data/diagnostics.csv   - subject_id, icd9_code
2. If you wanna change the acnhor list, change them in main.phenotypehasmap
3. Run the Program.