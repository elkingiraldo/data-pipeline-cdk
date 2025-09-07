import aws_cdk as core
import aws_cdk.assertions as assertions

from data_pipeline_cdk.data_pipeline_cdk_stack import DataPipelineCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in data_pipeline_cdk/data_pipeline_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DataPipelineCdkStack(app, "data-pipeline-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
