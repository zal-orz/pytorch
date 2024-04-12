import torch
from torchvision import models

# 设置设备
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 加载预训练的 VGG16 模型
vgg16 = models.vgg16(pretrained=True).to(device)

# 设置为评估模式
vgg16.eval()

# 创建随机的图像数据，VGG16模型接受的输入大小为 (batch_size, 3, 224, 224)
input_data = torch.randn(1, 3, 224, 224).to(device)

# 使用静态图的方式执行模型
scripted_model = torch.jit.trace(vgg16, input_data)

# 进行预测
with torch.no_grad():
    output = scripted_model(input_data)

# 打印输出
print(output)
