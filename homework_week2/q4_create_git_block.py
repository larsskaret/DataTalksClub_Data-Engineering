from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/larsskaret/DataTalksClub_Data-Engineering.git",

)
block.get_directory("homework_week2") # specify a subfolder of repo
block.save("question-4")

