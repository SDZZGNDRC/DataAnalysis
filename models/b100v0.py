from torch import nn
import torch.optim as optim

class B100v0(nn.Module):
    def __init__(self):
        super(B100v0, self).__init__()
        self.layer1 = nn.Linear(100, 1000)
        self.layer2 = nn.Linear(1000, 500)
        self.layer3 = nn.Linear(500, 50)
        self.layer4 = nn.Linear(50, 1)
        self.relu = nn.ReLU()

    def forward(self, x):
        x = self.relu(self.layer1(x))
        x = self.relu(self.layer2(x))
        x = self.relu(self.layer3(x))
        x = self.layer4(x)
        return x


# 创建一个实例
net = B100v0()
optimizer = optim.SGD(net.parameters(), lr=0.001, momentum=0.9)
# Print model's state_dict
print("Model's state_dict:")
for param_tensor in net.state_dict():
    print(param_tensor, "\t", net.state_dict()[param_tensor].size())

print()

# Print optimizer's state_dict
print("Optimizer's state_dict:")
for var_name in optimizer.state_dict():
    print(var_name, "\t", optimizer.state_dict()[var_name])
