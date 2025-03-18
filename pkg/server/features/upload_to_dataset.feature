Feature: Upload to dataset

  Scenario: Upload to an empty dataset
    When I create a new dataset with ID "my-dataset"
    And I upload a datapoint range containing 3 data points to the dataset with ID "my-dataset"
    Then the dataset should have 3 data points
    And the s3 bucket should contain the datapoint range

  Scenario: Upload to a dataset with existing data
    Given a dataset with ID "my-dataset" exists
    And the dataset contains 3 data points
    When I upload a datapoint range containing 3 data points ajdective to the existing datapoints
    Then the dataset should have 6 data points

  Scenario: Upload of overlapping datapoint ranges
    Given a dataset with ID "my-dataset" exists
    And the dataset contains 3 data points
    When I upload a datapoint range containing 3 data points overlapping with the existing datapoints
    Then the upload should fail with a 400 status code
    Then the dataset should have 3 data points

  Scenario: Upload of datapoint range with non-sequential datapoints
    Given a dataset with ID "my-dataset" exists
    When I upload a datapoint range containing 2 datapoints with keys 1 and 3
    Then the upload should fail with a 400 status code
    Then the dataset should have 0 data points
