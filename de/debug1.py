import torch
from torchvision import models
import torch.optim as optim
import torch.nn as nn

# 设置设备
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 加载预训练的 VGG16 模型
vgg16 = models.vgg16(pretrained=True).to(device)

# 设置为训练模式
vgg16.train()

# 创建优化器
optimizer = optim.SGD(vgg16.parameters(), lr=0.001, momentum=0.9)

# 创建损失函数
criterion = nn.CrossEntropyLoss()

# 创建随机的图像数据，VGG16模型接受的输入大小为 (batch_size, 3, 224, 224)
# input_data = torch.randn(1, 3, 224, 224).to(device)

# 使用静态图的方式执行模型
scripted_model = torch.jit.script(vgg16)

# 创建全零的假数据集
inputs = torch.zeros((1, 3, 224, 224)).to(device)
labels = torch.zeros(1, dtype=torch.long).to(device)

# 训练循环
for epoch in range(1):  # 假设我们训练1个epoch
    # 清零梯度
    optimizer.zero_grad()

    # 前向传播
    outputs = scripted_model(inputs)

    # 计算损失
    loss = criterion(outputs, labels)

    # 反向传播
    loss.backward()

    # 更新权重
    optimizer.step()

    print(f"Epoch {epoch+1} completed")
