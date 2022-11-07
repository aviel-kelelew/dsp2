# dsp2

Details about the work:
-Number if mapReduce rounds: 6
-Number of Step : 6
Instance: M4Large

How to run:
1. update the credential - on the local dir /.aws/credentials
2. create bucket- ass2bucket2
3. upload the jars to ass2bucket2 bucket
4. run the jar - java -jar  Ass2.0.jar


------- Map-Reduce jobs -----  

# JOB 1 -

Class:  CountPerDecade
-Using with 1-grams
Goal:
- Calculate for each decase his N
-Filter the Stop words .

Output:
-File : <Decade, N>
Mapper:
-Types: <LongWritable, Text, Text, LongWritable>
-output : 
          key- deccade
          value :  Long : N
Redducer:
-Types:   <Text,LongWritable,Text,LongWritable>



# JOB 2 -

Class: FirstJob
-Using with 1-grams

Goal:
-calculate for each w in the dataset her C (means number of occurrence
-join between the W,C to Decade,N
-Filter the Stop words .

Output:
File that contain:  <decade , w> < N, C1>

Mapper:
Types: <LongWritable, Text, KeyWordPerDecade, LongWritable>
output: k- <decade, w1,*>
          v- <numberofOccOf w>

Redducer:
Types: <KeyWordPerDecade,LongWritable,KeyWordPerDecade,LongWritable>
output:<decade,w1,*><N,number of occ w>


          
          
# JOB 3 -
class: SecondJob
-Using with 2-grams
Goal:
-Create sequence of the words with the number of their occ in for each decade

Output:<Decade,w1,w2><numberofocc of w1w2 =c12>

Mapper:
Types:  <LongWritable, Text, KeyWordPerDecade, LongWritable>
output:
K:<decade,w1,w2>   
v- <numberOf occ of w1w2=c12>

Redducer:
Types: Reducer<KeyWordPerDecade,LongWritable,KeyWordPerDecade,LongWritable>


          
          
# JOB 4 -
class: ThirdJob
using: - output of job 2 and job3
goal:- To do join between the two inputs

Output:  <decade,w1,w2><number of occ w1, number of occ w1w2, N>

Mapper:
Types:<LongWritable, Text, KeyForFirstJoin, Text>
input kind:
<decade,w1,*><N,numberofocc w1>
<decade,w1,w2><N, numberofcc w1w2>

output:
-<decade,w1,"a"><"from1gram",N,numbeof occ of w1>
<decade,w1,"b"><"from2gram",numberof occ of w1w2, w2>

Redducer:<KeyForFirstJoin, Text, Text, Text>
output:
<w2,w1,decade><numberofocc w1, number of occ w1w2 =c12 , N>

          
          
# JOB 5 -

class: JoinAllDetails
Using: output of job 4 and output of job2
Goals: -want to join to our couple the number of occ of W2
          -compute the log likehood ratio of each couplt for each decade.

Output: <Decade,W1,W2,Ratio>

Mapper:
Types: <LongWritable, Text, KeyForFirstJoin, Text> 
input kind:
<decade,w2,*><N,numberofocc w2>
<decade,w2,w1><N,number of w1, numberofcc w1w2>

output:
-<decade,w2,"a"><"from1gram",N,numbeof occ of w2>
<decade,w2,"b"><"from2gram",number of occ w1,numberof occ of w1w2, N>

Redducer:
Types: <KeyForFirstJoin, Text, Text, Text>

-If the input is from "from1gram" than we save the variable of number of occ of w2.
-Else, if the input is from "from2gram" we calcaulte the ratio of the couple word.

          
          
          
# JOB 6 -
class: ArrangingTheResult
using the output from job 5
Goal - send the 100 top ratio of couplt of word in each decade.


          
          
heb file:
Time runing with arrgretion:
- heb- all  : runing in 50 min.

Time runing without arrgretion: 1:00

english:
bad word:
1. A Word with number
2. A word that it not in english
3. A word with symbols
4.letter
5.word that they are name
6,Words that express contrast

Good Word:
1.adjectives
2.Words that belong to the period
3.noun
4.activation word
5.Words that express emotion

heb:
bad word:
1.words that used to say, but in the current relvent less
2.words with symbol
3.words with  "ה הידיעה"
4.words with private name
5. dates
6. countries
7.Words that express contrast

Good Word:
1.Words that belong to the period
2.adjectives
3.'words that symbol actions
4.words that belong to religion
5.Words that express emotion


English file:
Time runing with arrgretion: 5 hours

Time runing without arrgretion: 6 hours
