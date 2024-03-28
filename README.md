# Project 2 Report
Rafael Espinoza and Ahmad Choudry

## exact_F0 Results
### Local Execution:
- Time elapsed: 52s
- Estimate: 7,406,649

### GCP Execution:
- Time elapsed: 40s 
- Estimate: 7,406,649 

## exact_F2 Results (tug of war)
### Local Execution:
- Time elapsed: 71s
- Estimate: 8,567,966,130

### GCP Execution:
- Time elapsed: 55s 
- Estimate: 8,567,966,130 

## Tug_of_War Results
### Parameters: Width = 10, Depth = 3
### Local Execution:
- Time elapsed: 4s
- Estimate: 9,462,884,840

### GCP Execution:
- Time elapsed: 3s 
- Estimate: 9,462,884,840 

## BJKST Results
### Parameters: Width = 3, Trials = 5
### Local Execution:
- Time elapsed: 256s
- Estimate: 8,567,966,130

### GCP Execution (Predicted):
- Time elapsed: 180s - 200s
- Estimate: 8,567,966,130

## Comparison and Analysis
When comparing BJKST and Tug_of_War with the exact methods for computing F0 and F2, distinct differences in their operation and suitability for specific scenarios become apparent. BJKST encountered a challenge with the `z` value increasing too rapidly, requiring an adjustment to mitigate excessive inflation. This was a crucial fix, especially since running BJKST on a local machine with a full-sized CSV was highly time-consuming. As a result, we had to significantly reduce the file size for manageable processing times.

Tug_of_War, on the other hand, offered a more straightforward experience but also necessitated fine-tuning of the parameters to achieve a good balance between accuracy and processing time. The fundamental difference between the two lies in their approach to estimating distinct elements (F0) and second-frequency moments (F2), with Tug_of_War leaning towards a simpler, possibly less precise estimation process suited for quicker results with an acceptable margin of error.

Both algorithms present their advantages and drawbacks. Tug_of_War is ideal for scenarios requiring rapid estimates where some level of approximation is acceptable. Conversely, BJKST, with the necessary adjustments, offers greater accuracy for those who have the luxury of time or are working with smaller datasets.

Importantly, due to challenges in setting up the specified cluster configuration owing to cloud resource manager API issues, our professor permitted us to use a single-node processor. This adjustment significantly influenced our execution strategy and the interpretation of results.

In summary, both methods have their place depending on the size of the dataset and the precision required in the estimates. The choice between these algorithms highlights the importance of matching the tool to the task, considering the specific computational and accuracy requirements of F0 and F2 estimations.

