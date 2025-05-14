请按照以下格式组织你的测评环境:
.
├─answers/
├─data/
├─errors/
├─jars/
├─output/
├─strong/
├─standard.jar
并且按照自己本地的文件位置，修改checker最上方的BASE_DIR的值，并且确保generator和checker放在同一文件夹下。

选用一个jar作为标准答案，其他测试的 jar放在jars文件夹里面。如果想要跑自己的数据，把数据放在strong文件夹当中，测试的时候选择本地测试。(checker9.py功能不完善，只能在当前目录下新建一个叫MyData.txt的文本，其中存放本地数据)

注意每年的指导书都不相同，指令以及各种要求也不相同，必定需要修改才能使用。
