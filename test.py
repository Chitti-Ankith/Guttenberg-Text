from gutenbergdammit.ziputils import searchandretrieve

text = "Study"
total = 0

for info, text in searchandretrieve("gutenberg-dammit-files-v002.zip", {'Title': text}):
    total += len(text)
    # print(info['Title'][0], len(text))
          
print("Total: ", total)