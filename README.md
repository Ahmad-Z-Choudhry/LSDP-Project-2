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
When comparing BJKST and Tug_of_War with the exact methods, it's clear there are trade-offs. For BJKST, we hit a snag because the `z` value kept shooting up too fast. We had to tweak it down a bit to keep things under control. This fix was crucial, especially since running BJKST on a local machine took forever with a full-size CSV. We had to cut down the file size big time to get anything done.

Tug_of_War was a bit more straightforward but still needed some fine-tuning on the parameters to strike a good balance between getting accurate results and not having to wait ages for them. Both algorithms have their upsides and downsides, depending on what you're after. If you need quick estimates and can handle a bit of error, Tug_of_War might be the way to go. But if you're aiming for more accuracy and have the time (or a smaller dataset), then tweaking BJKST could work out better.

In a nutshell, both methods have their place, but it's all about finding the right tool for the job, considering the data size and how precise you need the estimates to be.


