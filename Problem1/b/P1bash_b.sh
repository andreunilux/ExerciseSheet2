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
create_folder "problem1_b"
tar -xf Wikipedia-En-41784-Articles.tar.gz -C ~/problem1_b
# Change to Folder
cd problem1_b
remove_folder "word-count"
remove_folder "word-pairs"
remove_folder "word-count100"
remove_folder "word-pairs100"



# Export classpath
export CLASSPATH=`hadoop classpath`:.:
echo $CLASSPATH

# Compile all java files
javac *.java

# Make java files executable
jar -cvf HadoopWordCount.jar HadoopWordCount*.class
jar -cvf TopNWordCount.jar TopNWordCount*.class
jar -cvf HadoopWordPairs.jar HadoopWordPairs*.class
jar -cvf TopNWordCountPairs.jar TopNWordCountPairs*.class
sleep 5

# Run the jar files


hadoop jar HadoopWordCount.jar HadoopWordCount enwiki-articles/AA/ word-count
hadoop jar TopNWordCount.jar TopNWordCount enwiki-articles/AA/ word-count100
echo "#####################"
echo "Finished  JOB 1/2"
echo "#####################"
hadoop jar HadoopWordPairs.jar HadoopWordPairs enwiki-articles/AA/ word-pairs
hadoop jar TopNWordCountPairs.jar TopNWordCountPairs enwiki-articles/AA/ word-pairs100
echo "#####################"
echo "Finished  JOB 2/2"
echo "#####################"


echo "Finished Jobs. You can now look at the results"












