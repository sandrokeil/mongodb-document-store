<?php
/**
 * This file is part of the proophsoftware/mongodb-document-store.
 * (c) 2018 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Prooph\EventMachine\MongoDb;

use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\Session;
use MongoDB\Driver\Exception\Exception as MongoDbException;
use Prooph\EventMachine\Persistence\DocumentStore;
use Prooph\EventMachine\Persistence\DocumentStore\Filter\Filter;
use Prooph\EventMachine\Persistence\DocumentStore\Index;
use Prooph\EventMachine\Persistence\DocumentStore\OrderBy\OrderBy;
use Prooph\EventMachine\MongoDb\Exception\InvalidArgumentException;
use Prooph\EventMachine\MongoDb\Exception\RuntimeException;

final class MongoDbDocumentStore implements DocumentStore
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $dbName;

    private $collectionPrefix = 'em_ds_';

    private $manageTransactions;

    /**
     * The transaction id, if currently in transaction, otherwise null
     *
     * @var Session|null
     */
    private $session;

    public function __construct(
        Client $client,
        string $dbName,
        string $collectionPrefix = null,
        bool $transactional = true
    ) {
        $this->client = $client;
        $this->dbName = $dbName;

        if ($collectionPrefix) {
            $this->collectionPrefix = $collectionPrefix;
        }

        $this->manageTransactions = $transactional;
    }

    public function listCollections(): array
    {
        $cursor = $this->client->selectDatabase($this->dbName)->listCollections(['filter' => ['name' => ['$regex' => '^' . $this->collectionPrefix . '.+']]]);

        $collections = [];

        foreach ($cursor as $item) {
            $collections[] = str_replace($this->collectionPrefix, '', $item->getName());
        }

        return $collections;
    }

    public function filterCollectionsByPrefix(string $prefix): array
    {
        $cursor = $this->client->selectDatabase($this->dbName)->listCollections(['filter' => ['name' => ['$regex' => '^' . $this->collectionPrefix . $prefix . '.+']]]);

        $collections = [];

        foreach ($cursor as $item) {
            $collections[] = str_replace($this->collectionPrefix, '', $item->getName());
        }

        return $collections;
    }

    public function hasCollection(string $collectionName): bool
    {
        return !empty(
        \iterator_to_array(
            $this->client->selectDatabase($this->dbName)->listCollections(
                ['filter' => ['name' => $this->collectionName($collectionName)]]
            )
        )
        );
    }

    public function addCollection(string $collectionName, Index ...$indices): void
    {
        $indicesCmds = array_map(function (Index $index) {
            return $this->indexToMongoCmd($index);
        }, $indices);

        $this->client->selectDatabase($this->dbName)->createCollection($this->collectionName($collectionName));

        if (!empty($indicesCmds)) {
            $this->getCollection($collectionName)->createIndexes($indicesCmds);
        }
    }

    public function dropCollection(string $collectionName): void
    {
        $this->getCollection($collectionName)->drop();
    }

    public function addDoc(string $collectionName, string $docId, array $doc): void
    {
        $doc['_id'] = $docId;

        try {
            $this->getCollection($collectionName)->insertOne($doc);
        } catch (MongoDbException $exception) {
            throw RuntimeException::fromMongoDbException($exception);
        }
    }

    public function updateDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $this->update($collectionName, $docId, $docOrSubset);
    }

    public function updateMany(string $collectionName, Filter $filter, array $set): void
    {
        $filter = $this->filterToWhereClause($filter);

        $options = [
            'projection' => ['_id' => 0],
        ];

        $this->transactional(function (Session $session) use ($collectionName, $options, $filter, $set) {
            $options['session'] = $session;
            $this->getCollection($collectionName)->updateMany(
                $filter,
                [
                    '$set' => $set,
                ],
                $options
            );
        });
    }

    public function upsertDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $this->update($collectionName, $docId, $docOrSubset, true);
    }

    public function deleteDoc(string $collectionName, string $docId): void
    {
        $this->getCollection($collectionName)->deleteOne(['_id' => $docId]);
    }

    public function deleteMany(string $collectionName, Filter $filter): void
    {
        $filter = $this->filterToWhereClause($filter);

        $options = [];

        $this->transactional(function (Session $session) use ($collectionName, $options, $filter) {
            $options['session'] = $session;
            $this->getCollection($collectionName)->deleteMany(
                $filter,
                $options
            );
        });
    }

    public function getDoc(string $collectionName, string $docId): ?array
    {
        return $this->getCollection($collectionName)->findOne(['_id' => $docId], ['projection' => ['_id' => 0]]);
    }

    public function filterDocs(
        string $collectionName,
        Filter $filter,
        int $skip = null,
        int $limit = null,
        OrderBy $orderBy = null
    ): \Traversable {
        $filter = $this->filterToWhereClause($filter);

        $options = [
            'projection' => ['_id' => 0],
        ];

        if ($limit) {
            $options['limit'] = $limit;
        }
        if ($skip) {
            $options['skip'] = $skip;
        }
        if ($orderBy) {
            $options['sort'] = $this->orderByToSort($orderBy);
        }

        return $this->getCollection($collectionName)->find($filter, $options);
    }

    private function update(string $collectionName, string $docId, array $docOrSubset, bool $upsert = false): void
    {
        $iterator = new \RecursiveIteratorIterator(new \RecursiveArrayIterator($docOrSubset));

        $docOrSubset = [];

        foreach ($iterator as $leafValue) {
            $keys = [];
            foreach (range(0, $iterator->getDepth()) as $depth) {
                $keys[] = $iterator->getSubIterator($depth)->key();
            }
            $docOrSubset[implode('.', $keys)] = $leafValue;
        }

        $this->getCollection($collectionName)->updateOne(

            [
                '_id' => $docId,
            ],
            [
                '$set' => $docOrSubset,
            ],
            [
                'upsert' => $upsert,
            ]
        );
    }

    /**
     * @param callable $callback
     * @throws \Throwable
     */
    private function transactional(callable $callback): void
    {
        if ($this->manageTransactions) {
            $this->session = $this->client->startSession();
            $this->session->startTransaction([
                'readConcern' => new \MongoDB\Driver\ReadConcern('snapshot'),
                'writeConcern' => new \MongoDB\Driver\WriteConcern(\MongoDB\Driver\WriteConcern::MAJORITY),
            ]);
        }

        try {
            $callback($this->session);
            if ($this->manageTransactions) {
                $this->session->commitTransaction();
                $this->session->endSession();
            }
        } catch (\Throwable $exception) {
            if ($this->manageTransactions) {
                $this->session->abortTransaction();
                $this->session->endSession();
            }
            throw $exception;
        } finally {
            $this->session = null;
        }
    }

    private function filterToWhereClause(Filter $filter, $argsCount = 0): array
    {
        if ($filter instanceof DocumentStore\Filter\AnyFilter) {
            if ($argsCount > 0) {
                throw new InvalidArgumentException('AnyFilter cannot be used together with other filters.');
            }
            return [];
        }

        if ($filter instanceof DocumentStore\Filter\AndFilter) {
            return [
                '$and' => [
                    $this->filterToWhereClause($filter->aFilter(), $argsCount),
                    $this->filterToWhereClause($filter->bFilter(), $argsCount),
                ],
            ];
        }

        if ($filter instanceof DocumentStore\Filter\OrFilter) {
            return [
                '$or' => [
                    $this->filterToWhereClause($filter->aFilter(), $argsCount),
                    $this->filterToWhereClause($filter->bFilter(), $argsCount),
                ],
            ];
        }

        switch (true) {
            case $filter instanceof DocumentStore\Filter\EqFilter:
                return [$filter->prop() => ['$eq' => $filter->val()]];
            case $filter instanceof DocumentStore\Filter\GtFilter:
                return [$filter->prop() => ['$gt' => $filter->val()]];
            case $filter instanceof DocumentStore\Filter\GteFilter:
                return [$filter->prop() => ['$gte' => $filter->val()]];
            case $filter instanceof DocumentStore\Filter\LtFilter:
                return [$filter->prop() => ['$lt' => $filter->val()]];
            case $filter instanceof DocumentStore\Filter\LteFilter:
                return [$filter->prop() => ['$lte' => $filter->val()]];
            case $filter instanceof DocumentStore\Filter\LikeFilter:
                return [$filter->prop() => ['$regex' => '^' . $filter->val() . '.+']];
            case $filter instanceof DocumentStore\Filter\NotFilter:
                $innerFilter = $filter->innerFilter();

                if (!$this->isPropFilter($innerFilter)) {
                    throw new RuntimeException('Not filter cannot be combined with a non prop filter!');
                }
                return [$innerFilter->prop() => ['$not' => $this->filterToWhereClause($innerFilter)[$innerFilter->prop()]]];
            case $filter instanceof DocumentStore\Filter\InArrayFilter:
                return [$filter->prop() => $filter->val()];
            case $filter instanceof DocumentStore\Filter\ExistsFilter:
                return [$filter->prop() => ['$exists' => true]];
            default:
                throw new RuntimeException('Unsupported filter type. Got ' . get_class($filter));
        }
    }

    private function isPropFilter(Filter $filter): bool
    {
        switch (get_class($filter)) {
            case DocumentStore\Filter\AndFilter::class:
            case DocumentStore\Filter\OrFilter::class:
            case DocumentStore\Filter\NotFilter::class:
                return false;
            default:
                return true;
        }
    }

    private function orderByToSort(DocumentStore\OrderBy\OrderBy $orderBy): array
    {
        $sort = [];

        if ($orderBy instanceof DocumentStore\OrderBy\AndOrder) {
            /** @var DocumentStore\OrderBy\Asc|DocumentStore\OrderBy\Desc $orderByA */
            $orderByA = $orderBy->a();
            $direction = $orderByA instanceof DocumentStore\OrderBy\Asc ? 1 : -1;
            $sort[$orderByA->prop()] = $direction;

            $sortB = $this->orderByToSort($orderBy->b());

            return array_merge($sort, $sortB);
        }

        /** @var DocumentStore\OrderBy\Asc|DocumentStore\OrderBy\Desc $orderBy */
        $direction = $orderBy instanceof DocumentStore\OrderBy\Asc ? 1 : -1;
        return [$orderBy->prop() => $direction];
    }

    private function indexToMongoCmd(Index $index): array
    {
        if ($index instanceof DocumentStore\FieldIndex) {
            $unique = $index->unique();
            $fields = $this->extractFieldPartFromFieldIndex($index);
            $name = $index->field();
        } elseif ($index instanceof DocumentStore\MultiFieldIndex) {
            $unique = $index->unique();
            $fields = [[]];
            foreach ($index->fields() as $field) {
                $fields[] = $this->extractFieldPartFromFieldIndex($field);
            }
            $fields = array_merge(...$fields);
            $name = implode('_',
                array_map(
                    function (DocumentStore\FieldIndex $field) {
                        return $field->field();
                    }, $index->fields())
            );
        } else {
            throw new RuntimeException('Unsupported index type. Got ' . get_class($index));
        }

        return [
            'key' => $fields,
            'unique' => $unique,
            'name' => $name,
            'background' => true,
        ];
    }

    private function extractFieldPartFromFieldIndex(DocumentStore\FieldIndex $fieldIndex): array
    {
        return [$fieldIndex->field() => $fieldIndex->sort() === Index::SORT_ASC ? 1 : -1];
    }

    private function collectionName(string $collectionName): string
    {
        return $this->collectionPrefix . $collectionName;
    }

    /**
     * Get mongo db stream collection
     *
     * @param string $collectionName
     * @return Collection
     */
    private function getCollection(string $collectionName): Collection
    {
        return $this->client->selectCollection(
            $this->dbName,
            $this->collectionName($collectionName),
            [
                'typeMap' => [
                    'root' => 'array',
                    'document' => 'array',
                    'array' => 'array',
                ],
            ]
        );
    }
}
