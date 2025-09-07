# Stage 1: Use an official lightweight Python image as a parent image
FROM python:3.11-slim

# Stage 2: Set the working directory inside the container
WORKDIR /app

# Stage 3: Copy the requirements file into the container
COPY requirements.txt .

# Stage 4: Install the Python dependencies
# --no-cache-dir keeps the image size smaller
RUN pip install --no-cache-dir -r requirements.txt

# Stage 5: Copy the rest of the backend application code into the container
COPY . .

# Stage 6: Expose the port the app runs on
EXPOSE 8000

# Stage 7: Define the command to run the application
# Use 0.0.0.0 to make the server accessible from outside the container
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]