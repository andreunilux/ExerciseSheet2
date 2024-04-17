#!/bin/bash


# Create folder function
create_folder() {
    local FOLDER="$1"
    
    # Check if the folder exists
    if [ -d "$FOLDER" ]; then
        echo "Folder $FOLDER already exists."
    else
        # Create the folder
        mkdir "$FOLDER"
        echo "Folder $FOLDER created successfully."
    fi
}


# Remove folder function
remove_folder() {
    local FOLDER="$1"
    
    # Check if the folder exists
    if [ -d "$FOLDER" ]; then
        echo "Folder $FOLDER already exists."
        rm "$FOLDER" -r
    else
        # Warning
        echo "Folder $FOLDER does not exist, cannot remove it."
    fi
}


#Create necessary folders
create_folder "problem1"
tar -xf Wikipedia-En-41784-Articles.tar.gz -C ~/problem1
# Change to Folder
cd problem1
remove_folder "word-count"
remove_folder "word-pairs"
remove_folder "word-stripes"




# Export classpath
export CLASSPATH=`hadoop classpath`:.:
echo $CLASSPATH

# Compile all java files
javac *.java
sleep 2
# Make java files executable
jar -cvf HadoopWordCount.jar HadoopWordCount*.class
jar -cvf HadoopWordPairs.jar HadoopWordPairs*.class
jar -cvf HadoopWordStripes.jar HadoopWordStripes*.class

# Run the jar files


hadoop jar HadoopWordCount.jar HadoopWordCount enwiki-articles/AA/ word-count
echo "#####################"
echo "Finished  JOB 1/3"
echo "#####################"
hadoop jar HadoopWordPairs.jar HadoopWordPairs enwiki-articles/AA/ word-pairs
echo "#####################"
echo "Finished  JOB 2/3"
echo "#####################"
hadoop jar HadoopWordStripes.jar HadoopWordStripes enwiki-articles/AA/ word-stripes
echo "#####################"
echo "Finished  JOB 3/3"
echo "#####################"

echo "Finished Jobs. You can now look at the results"












