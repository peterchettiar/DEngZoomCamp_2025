# bash script for downloading NYC taxi trip data 

# Ensure the script exits immediately if any command fails
set -e

# setting some variables - script expects 2 arguments
TAXI_TYPE=$1
YEAR=$2

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

# Loop through each month (1..12) - "%02d" ensure that month number is zero-padded
for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    # Construct the download URL
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

    # Define local paths
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "donwloading ${URL} to ${LOCAL_PATH}"

    # Create destination directory if it does not exist - p flag parent directory
    mkdir -p ${LOCAL_PREFIX}
    # Download file into specified path using -O flag
    wget ${URL} -O ${LOCAL_PATH}

done