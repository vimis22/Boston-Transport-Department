<h1>Boston Transport Department</h1>
<h2>The Objective</h2>
<p>
The project is apart of a 10 ECTS Course of Big Data and Science Technologies (E25). 
The objective with this course is to work different datasets where they overlap eachtother and thereby finding something valuable for the Customer.
Our main objective is to study the relationsship between transportation and weather.
Please note, that this project is also a part of our Scientific Methods Course (E25), where we will have a more theoretical approach there.
</p>

<h2>Who is our Customer</h2>
<p>Our Customer in this context is the Boston Transportation Department, since we are working with datasets of weather reports from different weather stations in Bostom City.</p>

<h2>How is our Architecture Structured?</h2>
<p>The following image shows how we have structured our image.</p>
<img src="src/assets/BD_Architecture.png">

<h2>Hive MVP Stack</h2>
<p>
We now include a self-contained Hive-on-HDFS environment (Docker-based) that aligns with the architecture diagram. The stack only covers Hive responsibilities (warehouse storage, metastore, HiveServer2) so it can be developed independently of Spark, Kafka, or dashboards.
</p>
<ul>
  <li>Configuration files live in <code>conf/</code>.</li>
  <li>Operational scripts (<code>bootstrap-hdfs.sh</code>, <code>init-metastore.sh</code>, smoke tests) live in <code>scripts/</code>.</li>
  <li><code>docker-compose.yml</code> orchestrates HDFS, the metastore DB, Hive Metastore, HiveServer2, and helper jobs.</li>
  <li>See <code>RUNBOOK.md</code> for end-to-end bootstrap and operations instructions.</li>
</ul>
