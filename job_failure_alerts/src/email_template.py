# html template for email body
failed_scripts_job_template = '''
<html>
<head>
    <style>
        body {{
            font-family: Arial, Helvetica, sans-serif;
            font-size: medium;
        }}

        h4 {{
            margin: 5px 0px;
        }}

        td {{
            margin: 10px;
        }}

        table {{
            border-collapse: collapse;
            font-size: medium;
        }}

        table,
        th,
        td {{
            border: 1px solid black;
            padding: 5px;
        }}

        .indented {{
            margin-left: 20px; /* Adjust the value as needed */
        }}

        th {{
            text-align: left; /* Left justify the column names */
        }}
    </style>
</head>
<body>
    <h2>{env} Failed Jobs for {resource_type} resource </h2>
    <div class="indented">
        <h4>Consecutive Failures:</h4>
        <table>
            <tr>
                <th>Source</th>
                <th>Consecutive Failures</th>
            </tr>
            {consec_fails_table_rows}
        </table>
        <br>
        <h4>Details:</h4>
        <table>
            <tr>
                <th>Source</th>
                <th>Protocol</th>
                <th>Provider</th>
                <th>Download ID</th>
                <th>Resource ID</th>
                <th>Incoming Time</th>
                <th>Status</th>
                <th>Event Description</th>
            </tr>
            {consec_fail_records_table_rows}
        </table>
    </div>
</body>
</html>
'''