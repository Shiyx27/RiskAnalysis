import pandas as pd
import os
from flask import Flask, request, render_template, send_file
from io import BytesIO  

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    global csv_data  
    if request.method == 'POST':
        file = request.files['file']
        if file:
            df = pd.read_csv(file)
            df['Order Creation Date'] = pd.to_datetime(df['Order Creation Date'], errors='coerce')
            df = df.sort_values(by=['Vehicle Number', 'Order Creation Date'], ascending=[True, True])
            
            df['Prev Manual End Odometer'] = df.groupby('Vehicle Number', group_keys=False).apply(
                lambda x: x.sort_values('Order Creation Date').shift(1))['Manual End Odometer (in meters)']
            
            def detect_risk(row):
                if pd.notna(row['Parent Vehicle Number']):
                    return row['Order Creation Date'], row['Vehicle Number'], None, None, 0
                
                risks = set()
                reasons = set()
                risk_value = 0
                
                if pd.notna(row['Prev Manual End Odometer']) and pd.notna(row['Manual Start Odometer (in meters)']):
                    if row['Manual Start Odometer (in meters)'] < row['Prev Manual End Odometer']:
                        risks.add("Odometer inconsistency")
                        reasons.add("Odometer reading is less than the previous day's end reading")
                        risk_value += 20
                
                if row['GPS Available'] == 'Yes':
                    if pd.notna(row['Trip GPS Distance Travelled (in KM)']) and pd.notna(row['Manual Distance Travelled (in KM)']):
                        if abs(row['Trip GPS Distance Travelled (in KM)'] - row['Manual Distance Travelled (in KM)']) > 0.1:
                            risks.add("GPS discrepancy")
                            reasons.add("GPS distance and manual distance differ significantly")
                            risk_value += 10
                
                if pd.notna(row['Manual Distance Travelled (in KM)']) and row['Manual Distance Travelled (in KM)'] > 125:
                    risks.add("Excessive travel distance")
                    reasons.add("Manual distance travelled exceeds 125 KM in a day")
                    risk_value += 15
                
                return row['Order Creation Date'], row['Vehicle Number'], '; '.join(risks) if risks else None, '; '.join(reasons) if reasons else None, risk_value
            
            df[['Date', 'Vehicle Number', 'Risk Factors', 'Reasoning', 'Risk Value']] = df.apply(detect_risk, axis=1, result_type='expand')

            deviations_df = df[df['Risk Factors'].notna()][['Zone', 'Hub', 'Vehicle Number', 'Date', 'Risk Factors', 'Reasoning', 'Risk Value']]

            grouped_deviations = deviations_df.groupby(['Zone', 'Hub', 'Vehicle Number']).agg({
                'Date': lambda x: ', '.join(x.astype(str)),
                'Risk Factors': lambda x: '; '.join(set(x)),
                'Reasoning': lambda x: '; '.join(set(x)),
                'Risk Value': 'sum'
            }).reset_index()
            
            impact_df = df.groupby('Date').apply(
                lambda x: (x['Manual Distance Travelled (in KM)'] - x['Trip GPS Distance Travelled (in KM)']).sum() / len(x)
            ).reset_index()
            impact_df.columns = ['Date', 'Impact Value']
            
            csv_buffer = BytesIO()
            grouped_deviations.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            global csv_data
            csv_data = csv_buffer.getvalue()

            top_20_hubs = grouped_deviations.groupby(['Zone', 'Hub']).agg({
                'Vehicle Number': lambda x: ', '.join(x.unique()),
                'Risk Value': 'sum'
            }).reset_index().sort_values(by='Risk Value', ascending=False).head(20)

            top_20_per_zone = grouped_deviations.sort_values(by=['Zone', 'Risk Value'], ascending=[True, False])
            top_20_per_zone = top_20_per_zone.groupby('Zone', group_keys=False).apply(lambda x: x.nlargest(20, 'Risk Value')).reset_index(drop=True)
            
            return render_template('index.html', top_20_hubs=top_20_hubs.to_dict(orient='records'),
                                   top_20_per_zone=top_20_per_zone.to_dict(orient='records'),
                                   grouped_data=grouped_deviations.to_dict(orient='records'),
                                   all_risk_data=grouped_deviations.to_dict(orient='records'),
                                   impact_data=impact_df.to_dict(orient='records'),
                                   file_ready=True)

    return render_template('index.html', top_20_hubs=[], top_20_per_zone=[], grouped_data=[], all_risk_data=[], impact_data=[], file_ready=False)

@app.route('/download')
def download_file():
    global csv_data
    if not csv_data:
        return "No data available", 400
    return send_file(BytesIO(csv_data), mimetype='text/csv', as_attachment=True, download_name="risk_analysis.csv")

if __name__ == '__main__':
    app.run(debug=True)
