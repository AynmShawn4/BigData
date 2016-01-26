<ol>
<li>For both pairs and stripes implementations, side data were required. <ul>
    <li><b>Count.java:</b> This program was first run to calculate the total line number and occurrence number of each word. The out file is already in the directory.
    Then, the output file will be stored and used as side data which will be loaded into setup function within reducer method for both PairsPMI and StripesPMI implementation. This program only has 1 MapReduce job. The input for this program is the shakespare text file; The intermediate key-value pairs are words and their occurrence number one; The final output records are total number of lines and occurrence numbers of words. </li>
    <li><b>PairsPMI.java:</b> Program with pairs implementation. It contains only 1 Mapreduce job.  The input for this program is the shakespare text file; The intermediate key-value pairs are pairs of words (x,y) and their occurrence number one; The final output records are pairs of words (x,y) and their PMI. At the reduce stage, side data from the output of Count.java is required.</li>
    <li><b>StripesPMI.java:</b> Program with stripes implementation. It contains only 1 Mapreduce job.  The input for this program is the shakespare text file; The intermediate key-value pairs are words and their associativeArray of occurrence number which is implemented as hashmap in program; The final output records are  words and their associativeArray of PMI. At the reduce stage, side data from the output of Count.java is required.</li>
    </ul>

</li>

<li>Running on Linux lab machine. <br>
    ParisPMI: 52.197 sec.<br>
    SripesPMI: 21.067 seconds.
</li>

<li>Running on Linux lab machine. <br>
    ParisPMI:  63.171 seconds <br>
    SripesPMI: 22.159 seconds.</li>

<li>984045 distinct pairs.</li>

<li>(loquitur, vir)	5.087987<br>
(lucifer's, privy-kitchen)	5.087987 <br>
(maidenliest, twinkled)	5.087987 <br>
(a-down-a, a-down)	5.087987 <br>
(palsies, dirt-rotten)	5.087987<br>


There are a huge number of pairs that have 5.087987 as PMI. However, most of them are realy rare and weird combinations. Since both x and y are quite rare, the p(x) * p(y) wil be really small compared to p(x,y) which makes p(x,y)/ (p(x) * p(y)) relatively large. 


</li>

<li>(tears, niobe)	2.6346688 <br>
(tears, distasted)	2.6346688<br>
(tears, cleansing)	2.6346688<br>
There are fair amount of pairs that have 2.6346688 as it PMI which is highest.<br>
<br>
(death, enernies) 2.1616447 <br>

(death, hard-favoured)	2.1616447<br>
(death, craved)	2.1616447<br>
There are fair amount of pairs that have 2.1616447 as it PMI which is highest.<br>

</li>
</ol>



Q4p			1.5

Q4s			0

Q5p			0

Q5s			0

Q6.1p		0

Q6.1s		0

Q6.2p		0

Q6.2s		0

Q7.1p		0

Q7.1s		0

Q7.2p		0

Q7.2s		0

linux p		4

linux s		4

alti p		0

alti s		0

notes		one day late

total		19.125

p stands for pair, s for stripe. linux p stands for run and compile pair in linux. 

If you have any question regarding to A1, plz come to DC3305 3~5pm on Friday (29th).
