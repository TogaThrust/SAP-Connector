require('dotenv').config();  // Load environment variables from .env file

const axios = require('axios');
const { Parser } = require('json2csv');
const { S3Client } = require('@aws-sdk/client-s3');

// Define the OData service URL
const baseUrl = `http://${process.env.HOST_IP}:${process.env.PORT}/sap/opu/odata/sap/RV_C_IOBJ_HIERARCHY_CDS/Rv_C_Iobj_Hierarchy`;

const filter = "?$expand=to_iobj,to_lastChangedBy,to_text,to_version";

// Function to fetch data from OData service
async function fetchData(url, filter) {
    let data = [];
    let nextUrl = url + filter;

    while (nextUrl) {
        try {
            // Make the GET request with Basic Authentication
            const response = await axios.get(nextUrl, {
                auth: {
                    username: process.env.SAP_USERNAME,
                    password: process.env.SAP_PASSWORD
                }
            });

            const jsonData = flattenCells(response.data.d.results);

            // Add the results from the current page
            data = data.concat(jsonData);

            // Check if there’s a next page (pagination)
            nextUrl = jsonData.__next || null;
        } catch (error) {
            console.error(`Error fetching data: ${error.response ? error.response.status : error.message}`);
            nextUrl = null;  // Stop the loop if there's an error
        }
    }

    return data;
}

function flattenObject(obj, prefix = '', skipKeys = []) {
    const result = {};
    for (let key in obj) {
        if (skipKeys.includes(key)) continue;

        const value = obj[key];
        const newKey = prefix ? `${prefix}.${key}` : key;
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
            Object.assign(result, flattenObject(value, newKey, skipKeys));
        } else {
            result[newKey] = value;
        }
    }
    return result;
}

function flattenCells(data, skipKeys = ['id', 'uri'], language = 'EN') {
    const allKeys = new Set();
    const flattenedRows = data.map(row => {
        const flatRow = {};
        for (let key in row) {
            const value = row[key];
            if (
                typeof value === 'object' &&
                value !== null &&
                !Array.isArray(value)
            ) {
                const flattened = flattenObject(value, key, skipKeys);
                Object.assign(flatRow, flattened);
                Object.keys(flattened).forEach(k => allKeys.add(k));
            } else if (Array.isArray(value) && key === 'to_text') {
                // Handle 'to_text' column with 'results' array
                if (value && value.results && Array.isArray(value.results)) {
                    value.results.forEach((item, index) => {
                        if (item.language && item.language.toUpperCase() === language.toUpperCase()) {
                            // Flatten the relevant fields from 'results', skipping metadata
                            const filteredItem = flattenObject(item, `${key}[${index}]`, skipKeys);
                            // Remove metadata fields if present
                            delete filteredItem['__metadata'];

                            Object.assign(flatRow, filteredItem);
                            Object.keys(filteredItem).forEach(k => allKeys.add(k));
                        }
                    });
                }
            } else {
                if (!skipKeys.includes(key)) {
                    flatRow[key] = value;
                    allKeys.add(key);
                }
            }
        }
        return flatRow;
    });

    // Fill missing keys with null
    const completedRows = flattenedRows.map(row => {
        const fullRow = {};
        allKeys.forEach(key => {
            fullRow[key] = key in row ? row[key] : null;
        });
        return fullRow;
    });

    return completedRows;
}

const s3DropObject = async (fileContent) => {
    const {
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION
    } = process.env;
    const BUCKET_NAME = 'adaptivetest-objectstorage';
    
    const s3 = new S3Client({
        region: AWS_REGION,
        credentials: {
            accessKeyId: AWS_ACCESS_KEY_ID,
            secretAccessKey: AWS_SECRET_ACCESS_KEY
        }
    });

    const params = {
        Bucket: BUCKET_NAME,
        Key: 'RV_C_IOBJ_HIERARCHY_CDS.csv',
        Body: fileContent,
        ContentType: 'text/csv'
    };

    try {
        const command = new PutObjectCommand(params);
        const result = await s3.send(command);
        console.log('✅ File uploaded successfully', result);
    } catch (err) {
        console.error('❌ Error uploading file:', err);
    }
};

// Main function to execute the script
async function main() {
    // const expandParams = '?$expand=to_iobj,to_lastChangedBy,to_text';
    const data = await fetchData(baseUrl, filter);
    if (data.length > 0) {
        const csvParser = new Parser();
        const csv = csvParser.parse(data);
        s3DropObject(csv);
    } else {
        console.log('No data retrieved');
    }
}

main().catch((err) => console.error('Error:', err));