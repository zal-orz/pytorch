import torch
print(torch.__path__)
# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


# class MyModel(torch.nn.Module):
#     def __init__(self):
#         super(MyModel, self).__init__()
#         self.fc1 = torch.nn.Linear(8000, 2000)
#         self.fc2 = torch.nn.Linear(2000, 1000)
#         self.relu = torch.nn.ReLU()
#         self.fc3 = torch.nn.Linear(1000, 100)

#     def forward(self, x):
#         x = self.fc1(x)
#         x = self.fc2(x)
#         x = self.relu(x)
#         return self.fc3(x)


# model = MyModel().to(device)

# input_data = torch.randn(8000, 8000).to(device)

# scripted_model = torch.jit.trace(model, input_data)

# output = scripted_model(input_data)

# print(output)
