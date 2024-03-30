#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ts_hashmap.h"

/**
 * Creates a new thread-safe hashmap.
 *
 * @param capacity initial capacity of the hashmap.
 * @return a pointer to a new thread-safe hashmap.
 */
ts_hashmap_t *initmap(int capacity)
{
  ts_hashmap_t *map = (ts_hashmap_t *)malloc(sizeof(ts_hashmap_t));
  map->table = (ts_entry_t **)calloc(capacity, sizeof(ts_entry_t *));
  map->lock = (pthread_mutex_t *)malloc(capacity * sizeof(pthread_mutex_t));

  for (int i = 0; i < capacity; i++)
  {
    pthread_mutex_init(&map->lock[i], NULL);
  }

  map->capacity = capacity;
  map->size = 0;
  map->numOps = 0;

  return map;
}

/**
 * Obtains the value associated with the given key.
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int get(ts_hashmap_t *map, int key)
{
  map->numOps++;
  int index = key % map->capacity;       // Compute the index for the key
  pthread_mutex_lock(&map->lock[index]); // Lock the mutex for the bucket

  ts_entry_t *entry = map->table[index]; // Get the head of the list at the bucket
  while (entry != NULL)
  { // Traverse the linked list
    if (entry->key == key)
    {                                          // Check if the curr entry's key == search key
      int value = entry->value;                // Store the value to return
      pthread_mutex_unlock(&map->lock[index]); // Unlock the mutex
      return value;                            // Return the found value
    }
    entry = entry->next; // Move to the next entry in the list
  }

  pthread_mutex_unlock(&map->lock[index]); // Unlock the mutex if key is not found
  return INT_MAX;                          // Return INT_MAX if the key is not found
}

/**
 * Associates a value associated with a given key.
 * @param map a pointer to the map
 * @param key a key
 * @param value a value
 * @return old associated value, or INT_MAX if the key was new
 */
int put(ts_hashmap_t *map, int key, int value)
{
  map->numOps++;
  int index = key % map->capacity;

  pthread_mutex_lock(&map->lock[index]); // Lock the mutex for the bucket

  // Get the head of list
  ts_entry_t *cur_entry = map->table[index];

  // Allocate memory for the new entry
  ts_entry_t *new_entry = (ts_entry_t *)malloc(sizeof(ts_entry_t));

  // Set the key, value, and next pointer of the new entry
  new_entry->key = key;
  new_entry->value = value;
  new_entry->next = NULL;

  // If the bucket is empty
  if (cur_entry == NULL)
  {
    // Set the new entry as the head of the list
    map->table[index] = new_entry;
    // Increment the size of the map
    map->size++;
    // Unlock the mutex
    pthread_mutex_unlock(&map->lock[index]);
    // Return INT_MAX to indicate success
    return INT_MAX;
  }

  // Traverse the linked list
  while (cur_entry != NULL)
  {
    // If the key already exists
    if (cur_entry->key == key)
    {
      // Store the old value
      int old_value = cur_entry->value;
      // Update the value
      cur_entry->value = value;
      // Free the memory allocated for the new entry
      free(new_entry);
      // Unlock the mutex
      pthread_mutex_unlock(&map->lock[index]);
      // Return the old value
      return old_value;
    }
    // If we reached the end of the list
    if (cur_entry->next == NULL)
    {
      // Insert the new entry at the end
      cur_entry->next = new_entry;
      // Increment the size of the map
      map->size++;
      // Unlock the mutex
      pthread_mutex_unlock(&map->lock[index]);
      // Return INT_MAX to indicate success
      return INT_MAX;
    }
    // Move to the next entry in the list
    cur_entry = cur_entry->next;
  }

  // Unlock the mutex
  pthread_mutex_unlock(&map->lock[index]);
  // Return -1 if it fails
  return -1;
}

/**
 * Removes an entry in the map
 * @param map a pointer to the map
 * @param key a key to search
 * @return the value associated with the given key, or INT_MAX if key not found
 */
int del(ts_hashmap_t *map, int key)
{
  map->numOps++;
  int out;
  int index = key % map->capacity;
  pthread_mutex_lock(&map->lock[index]);
  ts_entry_t *entry = map->table[index];
  ts_entry_t *prev = NULL;

  if (entry == NULL)
  {
    pthread_mutex_unlock(&map->lock[index]);
    return INT_MAX;
  }

  if (entry->key == key) // incase the key is found at the beginning of hte list
  {
    map->size--;
    out = entry->value;
    map->table[index] = entry->next;
    free(entry);
    pthread_mutex_unlock(&map->lock[index]);
    return out; // target key at index head
  }

  // Traverse the linked list to find the key
  while (entry != NULL && entry->key != key)
  {
    prev = entry;
    entry = entry->next;
  }

  // If the key is not found
  if (entry == NULL)
  {
    // Unlock the mutex and return INT_MAX to indicate key not found
    pthread_mutex_unlock(&map->lock[index]);
    return INT_MAX;
  }

  // Store the value of the entry to return later
  int value = entry->value;

  // Update the previous entry's next pointer to skip the entry
  prev->next = entry->next;

  free(entry);

  map->size--;

  pthread_mutex_unlock(&map->lock[index]);

  // Return the value of the deleted entry
  return value;
}

/**
 * Prints the contents of the map (given)
 */
void printmap(ts_hashmap_t *map)
{
  for (int i = 0; i < map->capacity; i++)
  {
    printf("[%d] -> ", i);
    ts_entry_t *entry = map->table[i];
    while (entry != NULL)
    {
      printf("(%d,%d)", entry->key, entry->value);
      if (entry->next != NULL)
        printf(" -> ");
      entry = entry->next;
    }
    printf("\n");
  }
}

/**
 * Free up the space allocated for hashmap
 * @param map a pointer to the map
 */
void freeMap(ts_hashmap_t *map)
{
  for (int i = 0; i < map->capacity; i++)
  {
    ts_entry_t *entry = map->table[i];
    // Free memory for each entry in the linked list
    while (entry != NULL)
    {
      // Store the next pointer before freeing the current entry
      ts_entry_t *next = entry->next;
      free(entry);  // Free the memory for the current entry
      entry = next; // Move to the next entry
      map->size--;
    }
    // Destroy the mutex associated with the bucket
    pthread_mutex_destroy(&map->lock[i]);
  }
  // Free the memory allocated for the lock array and table array
  free(map->lock);
  free(map->table);

  // Free the memory allocated for the hashmap itself
  free(map);
}