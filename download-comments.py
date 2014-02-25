import urllib.request, urllib.error, urllib.parse
import json
import pytz
import re
import numpy
import pandas as pd
from pandas import DataFrame


class Tree(object):
   def __init__(self, data):
      self.children = None
      self.data = data

   def addChild(self, node):
      if self.children is None:
         self.children = [node]
      else:
         self.children.append(node)


def commentTree(jsonNode):
   keys = ['id', 'author', 'text', 'points', 'created_at']
   nodeData = dict((k, jsonNode[k]) for k in keys)
   nodeData['text'].strip().replace(',', '').replace('"',"'")

   node = Tree(nodeData)
   for child in jsonNode['children']:
      node.addChild(commentTree(child))

   return node


def nodeToRecord(node, storyId, parentId):
   keys = ['id', 'author', 'text', 'points', 'created_at']
   record = [node.data[k] for k in keys] + [parentId, storyId]
   return record



def preorderTraversal(node, storyId, parentId):
   comments = [nodeToRecord(node, storyId, parentId)]

   if node.children is None:
      return comments

   for child in node.children:
      comments.extend(preorderTraversal(child, storyId, node.data['id']))

   return comments


def preorderTraversalIgnoreRoot(commentTree):
   storyId = commentTree.data['id']

   if commentTree.children is None:
      return []

   comments = []
   for node in commentTree.children:
      comments.extend(preorderTraversal(node, storyId, None))

   return comments


# return a tree of comments for the given story
def commentsForStory(objectId):
   url = 'https://hn.algolia.com/api/v1/items/%d' % (objectId)

   req = urllib.request.Request(url)
   response = urllib.request.urlopen(req)
   data = json.loads(response.read().decode("utf-8"))

   tree = commentTree(data)
   commentRecords = preorderTraversalIgnoreRoot(tree)

   columns = ['id', 'author', 'text', 'points', 'created_at', 'parent_id', 'story_id']
   df = DataFrame(columns = columns, index = numpy.arange(len(commentRecords)))
   for index, comment in enumerate(commentRecords):
      df.ix[index] = comment

   df.to_csv("comments-by-story/comments-%d.csv" % objectId, encoding='utf-8', index=False)


if __name__ == "__main__":
   commentsForStory(1)