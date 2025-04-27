FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt consumer_controller.py ./  

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["/bin/bash", "-c"]

#CMD ["python consumer_controller.py"]

# Override to keep the container running: 
#CMD ["python consumer_controller.py && exec bash"]
CMD ["exec bash"]