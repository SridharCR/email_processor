# Email rule processor

This project enables us to pull the email data and its metadata and load them in any database of our choice. After that,
we can apply rules for the emails with matching conditions to do appropriate actions.

## Email database design

A simple database schema to hold the required data for the computations

![db_design.png](design%2Fdb_design.png)

## Code

The script is developed in a generic fashion, so that it will be easy to understand and as well easy to extend the
functionality to bring in additional features.

The structure of the code is defined as follows,

- lib
- src
    - rule_processor
        - dao
        - middlewares
- tests

The code has been formatted with black and adheres to the pylint standards.

- black: `All done! ✨ 🍰 ✨`
- pylint: `Your code has been rated at 8.45/10`

## Setup

1. Setup an venv/conda environment and install the requirements.txt
    ```{console}
   python -m venv venv
   source venv/bin/activate && pip install -r requirements.txt
   ```
2. Setup the python path
    ```{console}
   export PYTHONPATH=$PYTHONPATH:./
   ```
3. Setup the database for this activity
    ```{console}
   docker-compose up -d 
   ```
3. Run the main orchestrator.py with different modes based on your needs.
    ```{console}
    python src/rule_processor/orchestrator.py --db-cleanup True --import-email True --rule-engine True
    ```

## Samples

Here's an sample scenario mentioned in the document
![Screenshot from 2023-05-18 08-42-28.png](logs%2FScreenshot%20from%202023-05-18%2008-42-28.png)

Entire log information available here - [sample_usecase.log](logs%2Fsample_usecase.log)

## Author

- [SridharCR](https://github.com/SridharCR)