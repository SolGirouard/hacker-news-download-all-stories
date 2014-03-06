import urllib.request, urllib.error, urllib.parse
import json
import pytz
import re
import time
import traceback
import numpy
import pandas as pd
from pandas import DataFrame


SUCCESS = 'Downloaded successfully'
NO_COMMENTS = 'No comments'
RETRY_FAILURES = True


def getLog():
   if not os.path.isfile('comments.log'):
      from subprocess import call
      call(['touch', 'comments.log'])

   with open('comments.log', 'r') as infile:
      return dict([x.strip().split('\t') for x in infile.readlines()])


def writeLog(log):
   with open('comments.log', 'w') as outfile:
      for (objectId, value) in log.items():
         outfile.write('%s\t%s\n' % (objectId, value))


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

   if nodeData['text'] is not None:
      nodeData['text'] = nodeData['text'].strip().replace('"',"'")

   node = Tree(nodeData)
   for child in jsonNode['children']:
      # some nodes appear to have no authors due to comment deletion
      # ignore these subtrees
      if all([x in child for x in keys]):
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
def commentsForStory(objectId, log):
   try:
      url = 'https://hn.algolia.com/api/v1/items/%d' % (objectId)

      req = urllib.request.Request(url)
      response = urllib.request.urlopen(req)
      data = json.loads(response.read().decode("utf-8"))
   except (KeyboardInterrupt, SystemExit):
      raise
   except IOError as e:
      message = '%d: %s' % (e.code, e.reason)
      log[str(objectId)] = message
      print(message)
      return

   tree = commentTree(data)
   commentRecords = preorderTraversalIgnoreRoot(tree)

   if len(commentRecords) == 0:
      log[str(objectId)] = NO_COMMENTS
      return

   columns = ['id', 'author', 'text', 'points', 'created_at', 'parent_id', 'story_id']
   df = DataFrame(columns = columns, index = numpy.arange(len(commentRecords)))
   for index, comment in enumerate(commentRecords):
      df.ix[index] = comment

   df.to_csv("comments-by-story/comments-%d.csv" % objectId, encoding='utf-8', index=False)
   log[str(objectId)] = SUCCESS


isFailure = lambda log, k: log[k] != SUCCESS and log[k] != NO_COMMENTS
is404 = lambda log, k: '404' in log[k]
def shouldTry(log, key):
   return key not in log or (RETRY_FAILURES and isFailure(log, key) and not is404(log, key))


def processAllComments(stories, log):
   for i, story in enumerate(stories):
      storyId = int(story[0])
      filename = 'comments-by-story/comments-%d.csv' % storyId

      if os.path.isfile(filename): # or log[str(storyId)] == SUCCESS:
         print('skipped %d, file exists' % storyId)
         log[str(storyId)] = SUCCESS
         continue

      if shouldTry(log, str(storyId)):
         print('\t%d\t%.2f%%' % (storyId, 100 * i / len(stories)))
         commentsForStory(storyId, log)
         time.sleep(3.7)
      else:
         print('skipped %d, either already in log or a past failure' % storyId)

      if i % 1000 == 0:
         writeLog(log)
         print('Wrote log on %d-th story' % i)


if __name__ == "__main__":
   import sys
   import os.path

   if len(sys.argv) != 2:
      print('Usage: python download-comments.py path/to/stories.csv')
      sys.exit(-1)

   storyDatabase = sys.argv[-1]
   log = getLog()

   with open(storyDatabase, 'r') as infile:
      stories = [line.strip().split(',') for line in infile.readlines()[1:]]

   print('Processing...')
   try:
      processAllComments(stories, log)
   finally:
      writeLog(log)

