from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/larsskaret/DataTalksClub_Data-Engineering.git",

)
#block.get_directory("folder-in-repo") # specify a subfolder of repo
block.save("question-4")

