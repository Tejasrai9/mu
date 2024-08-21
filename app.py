from flask import Flask, request, render_template, send_file
import pandas as pd
import mysql.connector
import os

app = Flask(__name__)

def fetch_scheme_data_from_db(scheme_name, cursor):
    query = "SELECT * FROM July WHERE `Scheme_Name` = %s"
    cursor.execute(query, (scheme_name,))
    result = cursor.fetchone()  # Fetch only one row
    cursor.fetchall()  # Clear any remaining results to avoid "Unread result" error
    return result

def fill_missing_data(input_file):
    host = '195.100.100.197'
    user = 'root'
    password = 'server1'
    database = 'prototypedb1'

    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    cursor = conn.cursor(dictionary=True)
    df = pd.read_excel(input_file)

    for index, row in df.iterrows():
        scheme_name = row['Scheme Name']

        if pd.isna(row['Trail 1st yr']) or pd.isna(row['Trail 2nd yr']) or pd.isna(row['Trail 3rd Yr']) or pd.isna(row['Trail 4th Yr']):
            db_data = fetch_scheme_data_from_db(scheme_name, cursor)
            
            if db_data:
                # Explicitly cast to float if possible
                df.at[index, 'Trail 1st yr'] = float(db_data.get('Trail_1st_yr', row['Trail 1st yr'])) if db_data.get('Trail_1st_yr') else row['Trail 1st yr']
                df.at[index, 'Trail 2nd yr'] = float(db_data.get('Trail_2nd_yr', row['Trail 2nd yr'])) if db_data.get('Trail_2nd_yr') else row['Trail 2nd yr']
                df.at[index, 'Trail 3rd Yr'] = float(db_data.get('Trail_3rd_Yr', row['Trail 3rd Yr'])) if db_data.get('Trail_3rd_Yr') else row['Trail 3rd Yr']
                df.at[index, 'Trail 4th Yr'] = float(db_data.get('Trail_4th_Yr', row['Trail 4th Yr'])) if db_data.get('Trail_4th_Yr') else row['Trail 4th Yr']

    df.to_excel(input_file, index=False)
    cursor.close()
    conn.close()

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # Check if the POST request has the file part
        if 'file' not in request.files:
            return 'No file part'
        
        file = request.files['file']
        
        if file.filename == '':
            return 'No selected file'
        
        if file:
            # Save the file to a temporary location
            file_path = os.path.join('uploads', file.filename)
            file.save(file_path)
            
            # Process the file
            fill_missing_data(file_path)
            
            # Send the file back to the user
            return send_file(file_path, as_attachment=True)
    
    return render_template('upload.html')

if __name__ == '__main__':
    # Create the uploads directory if it doesn't exist
    os.makedirs('uploads', exist_ok=True)
    app.run(debug=True)
