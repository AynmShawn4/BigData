The following results are from Altiscale<br>
1 <br>
x: 1-ROCA%: 17.25  
  y: 12.82 <br>
  britney: 1-ROCA%: 19.06
  
2<br> 
1-ROCA%: 11.98

3<br> 1-ROCA%: 15.80

4<br> 1-ROCA%: 13.17

5 <br>
1-ROCA%: 19.80  

1-ROCA%: 14.90  

1-ROCA%: 15.65 

1-ROCA%: 19.23

1-ROCA%: 16.69

1-ROCA%: 14.95

1-ROCA%: 19.31

1-ROCA%: 18.45

1-ROCA%: 15.97

1-ROCA%: 18.01

average: 17.30

Marks:
Compilation: 4/4
TrainSpamClassifier: 14/15
ApplySpamClassifier: 5/5
ApplyEnsembleClassifier: 6/6
Shuffle implementation: 5/5
Question Answers: 15/15
Runs on Altiscale: 10/10
Total: 59/60

- Voting was much more complex than necessary
- Weight map in training needs only to be local to the flatMap/mapPartitions, could cause problems if we attempted to use w in a more complex application (i.e., training + classification in the same program)
