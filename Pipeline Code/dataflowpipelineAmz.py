import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from apache_beam.options.pipeline_options import PipelineOptions
import google.auth
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState
from IPython.core.display import display, HTML

def Split(element):
    return element.split(",")

# Finding the average of given list of values
class avgRatingFn(beam.CombineFn):

    def create_accumulator(self):
        return (0.0, 0)  # initialize (sum, count)

    def add_input(self, sum_count, input1):
        (sum1, count) = sum_count
        (sum2, c2) = input1
        return sum1 + float(sum2) * c2, count + c2

    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators)
        return sum(ind_sums), sum(ind_counts)

    def extract_output(self, sum_count):
        (sum1, count) = sum_count
        return sum1 / count if count else float('NaN')

pipe = beam.Pipeline(InteractiveRunner())

# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=[])

# Sets the project to the default project in the current Google Cloud environment.
_, options.view_as(GoogleCloudOptions).project = google.auth.default()
options.view_as(GoogleCloudOptions).region = 'us-west2'
dataflow_gcs_location = 'gs://bucket-amazonn/dataflowAmz'

options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % dataflow_gcs_location

# The Dataflow Temp Location location is used to store intermediate results before outputting the final result.
options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % dataflow_gcs_location

clientBQ = bigquery.Client()

dataset_id = "keyskills-1637552843696.dataflowpipelineAmz"
dataset = bigquery.Dataset(dataset_id)
dataset.location = "us-west2"
dataset.description = "Amazon dataset books"
clientBQ.create_dataset(dataset, timeout = 30)

#Convert csv to json
def to_json(csv_file):
    fields = csv_file.split(',')
    json_file = {"Name": fields[0],
                "Author": fields[1],
                "User_Rating": fields[2],
                "Reviews": fields[3],
                "Price": fields[4],
                "Year": fields[5],
                "Genre": fields[6]
                }
    return json_file

table_schema = 'Name:STRING,Author:STRING,User_Rating:FLOAT,Reviews:INTEGER,Price:Integer,Year:Integer,Genre:STRING'

bookAmz = (pipe | beam.io.ReadFromText("gs://bucket-amazonn/clean_books_amazon.csv/part-00000-44653796-5293-4a2a-b34e-225c74788cd8-c000.csv"))
(bookAmz | 'cleaned_data to json' >> beam.Map(to_json)
 | 'write to bigquery' >> beam.io.WriteToBigQuery(
            "keyskills-1637552843696:dataflowpipelineAmz.tableAmz",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            custom_gcs_temp_location="gs://bucket-amazonn/dataflowAmz/temp"
        )
 )

pipelineAmz = pipe.run()
if pipelineAmz.state == PipelineState.DONE:
    print('The pipeline is running successfully \n The table is sent to big query successfully !!!')
else:
    print('Error running the pipeline')

# Reads data and split based on ‘,’
pipe2 = beam.Pipeline(InteractiveRunner())
AmzBooks = (pipe2 | beam.io.ReadFromText("gs://bucket-amazonn/clean_books_amazon.csv/part-00000-44653796-5293-4a2a-b34e-225c74788cd8-c000.csv") | beam.Map(Split))

# Filter records having fiction. Map each rating as a set (rating,1) .Using combineperkey to count the number of each rating. Running the average function and write the result to Fiction_result1
avg_fict = (
        AmzBooks
        | beam.Filter(lambda rec: rec[6] == "Fiction")
        | beam.Map(lambda rec: (rec[2], 1))
        | "Fict Combine keys1" >> beam.CombinePerKey(sum)
        | "Fict Combine Global keys1" >> beam.CombineGlobally(avgRatingFn())
        | "Fict Write to  bucket1" >> beam.io.WriteToText("gs://bucket-amazonn/dataflowAmz/FictionAvg_Result")
)

#Same for "non fiction"
avg_non_fict = (
        AmzBooks
        | beam.Filter(lambda rec: rec[6] == "Non Fiction")
        | beam.Map(lambda rec: (rec[2], 1))
        | "N_Fict Combine keys" >> beam.CombinePerKey(sum)
        | "N_Fict Combine Global keys" >> beam.CombineGlobally(avgRatingFn())
        | "N_Fict Write to bucket" >> beam.io.WriteToText("gs://bucket-amazonn/dataflowAmz/NonFictionAvg_Result")
)

#Same for "all genre"
avg_all = (
        AmzBooks
        | beam.Map(lambda rec: (rec[2], 1))
        | "All Combine keys" >> beam.CombinePerKey(sum)
        | "All Combine Global keys" >> beam.CombineGlobally(avgRatingFn())
        | "All Write to bucket" >> beam.io.WriteToText("gs://bucket-amazonn/dataflowAmz/AllBookAvg_Result")
)

# Map each record’s 0th column that is name with value 1.
# Running top.of(5) to sort and get the last5 books alphabetically and store in storage bucket.
last5_alpha_order = (AmzBooks | beam.Map(lambda rec: (rec[0], 1)) | beam.Distinct() | beam.combiners.Top.Of(5) | beam.io.WriteToText("gs://bucket-amazonn/dataflowAmz/Last5_Result"))

dataflow_pipeline_Amz = DataflowRunner().run_pipeline(pipe2, options=options)

url = ('https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s' %
       (dataflow_pipeline_Amz._job.location, dataflow_pipeline_Amz._job.id, dataflow_pipeline_Amz._job.projectId))
display(HTML('Click <a href="%s" target="_new">here</a> for the details of your Dataflow job!' % url))
