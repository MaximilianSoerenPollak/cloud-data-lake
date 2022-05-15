<div id="top"></div>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->



<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
<div align="center">

[![LinkedIn][linkedin-shield]][linkedin-url]

</div>


<h3 align="center">Cloud Data Lake</h3>

  <p align="center">
    This project is my solution to the `Udacity Data Engineering Nanodegree Cloud Data Lake Project`.
    <br />
    A datalake that is build with the help of python. It automatically spins up an S3 bucket downloads/cleans and uploads the data. 
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
    </li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

<br/>
This is an ETL that automatically makes an S3 bucket and saves data into there.
This data is taken from public S3 buckets hosted by udacity.
<br/>
This project was quite fun to make, and taught me lot about Infrastructure as code, AWS, ETL and parquets.


#### Why this approach & why cloud?
This approach with downloading data then wrangling it and uploading it again 
was taken so that we only have to have an S3 bucket, and no EC2 machines to pay for.
And the easy part is that we can upgrade to EC2 machines in the future if necessary.

The cloud was chosen to give easy expandability in the future as well as uptime and low overhead  
in terms of not needing on-perm hardware.

#### Why this ETL design?
The ETL is designed to work with the current infrastructure and be easily useable by anybody  
with just a little bit of tech knowledge. All that is needed is to clone the repo and execute two commands  
(and entering some configs). 

<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [Python](https://www.python.org/)
* [AWS](https://aws.amazon.com/)


<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This is an example of how can get the project working from your machine.
Be mindful that this will make a S3 Bucket with your AWS account.

### Prerequisites
 * [Python](www.python.org)
 * [AWS-Acount](https://aws.amazon.com/)
 * Python virtualenv
   ```sh
   pip install virtualenv 
   ```

### Installation
1. Clone the repo of the branch you want.
   ```sh
   git clone https://github.com/maximiliansoerenpollak/cloud-data-warehouse
   ```

2. Open a terminal and navigate to the folder where you cloned the repo and make a virtual environment.
   ```sh
      cd place/you/cloned/repo/cloud-data-lake
   ```
   Activate and install all requirements
   
   ```sh
      python3 -m venv name_of_virtualenv
      source name_of_virtualenv/bin/activate
      pip -r install requirements.txt
   ```
   Now you should have all requirements installed that are needed for the project.

3. You first have to open up the dl-template.cfg and fill out all the input there.
   Make sure you create a new IAM role in your AWS account since you do not want
   to enter your admin accounts information.The ACCESS and SECRET key from that 
   IAM role are to be filled in here.

   ```
    [KEYS]
    AWS_ACCESS_KEY=
    AWS_SECRET_KEY=

    [DATA]
    SONG_DATA=s3://udacity-dend/song_data
    LOG_DATA=s3://udacity-dend/log_data

    [AWS]
    S3_BUCKET_NAME=anameforyourbucket
   ```
    Once you have filled this out with the correct information, save it as `dl.cfg`.

4.  After you have saved the config as `dl.cfg` and filed it all in you can start the process.
    All you have to do is to go into the folder where you cloned the project and run the start script.
    ```sh
    #Make the shellscript exectuable and start it 
    chmod +x doall.sh
    ./doall.sh
    ```
    It will then prompt you to enter the S3 bucket name. Just enter the same name you have entered as the name in the dl.cfg file.
    It should then go through the whole process. 

5. If you want to shut down all created AWS resources, just run `python aws_shutdown.py`.
   This will delete the S3 bucket and all it's contents


<p align="right">(<a href="#top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Maximilain Soeren Pollak - pollakmaximilian@gmail.com

Project Link: [https://github.com/maximiliansoerenpollak/portfolio-api](https://github.com/maximiliansoerenpollak/portfolio-api)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[license-shield]: https://img.shields.io/github/license/maximiliansoerenpollak/portfolio-api
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/msoerenpollak

